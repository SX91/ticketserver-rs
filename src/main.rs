#![feature(conservative_impl_trait)]
#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate futures;
extern crate hyper;
extern crate multimap;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::rc::Rc;
use std::cell::RefCell;
use std::fmt;

use futures::future::{self, Future};
use futures::stream::Stream;
use hyper::{header, Body, Chunk, Method, StatusCode};
use hyper::server::{Http, Request, Response, Service};
use serde::{Deserialize, Deserializer};
use serde::de::Visitor;
use multimap::MultiMap;

pub type Timestamp = i64;
pub type Price = u64;

const DELTA_MIN: Timestamp = 3 * 60 * 60;
const DELTA_MAX: Timestamp = 8 * 60 * 60;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize)]
pub struct ID(String);

impl ID {
    const SIZE: usize = 32;

    pub fn new(id: &str) -> Option<Self> {
        if id.len() == Self::SIZE {
            Some(ID(id.to_owned()))
        } else {
            None
        }
    }
}

impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct IDVisitor;
        impl<'de> Visitor<'de> for IDVisitor {
            type Value = ID;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("ID string (32 characters)")
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
                where E: serde::de::Error
            {
                self.visit_str(&v)
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: serde::de::Error
            {
                if let Some(id) = ID::new(v) {
                    Ok(id)
                } else {
                    Err(E::custom("invalid ID length (must be 32)"))
                }
            }
        }
        deserializer.deserialize_str(IDVisitor)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Serialize, Deserialize)]
pub struct AirportCode(String);

#[derive(Debug, Serialize, Deserialize)]
pub struct Ticket {
    pub id: ID,
    pub departure_code: AirportCode,
    pub arrival_code: AirportCode,
    pub departure_time: Timestamp,
    pub arrival_time: Timestamp,
    pub price: Price,
}

#[derive(Serialize, Deserialize)]
pub struct InsertRequest {
    pub tickets: Vec<Ticket>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Success,
    Failure,
}

#[derive(Serialize, Deserialize)]
pub struct InsertResponse {
    pub status: Status,
}

impl InsertResponse {
    pub fn new(status: Status) -> Self {
        InsertResponse { status: status }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SearchRequest {
    pub departure_code: AirportCode,
    pub arrival_code: AirportCode,
    pub departure_time_start: Timestamp,
    pub departure_time_end: Timestamp,
}

#[derive(Serialize)]
pub struct TicketStack<'a> {
    pub ticket_ids: [&'a ID; 2],
    pub price: Price,
}

impl<'a> TicketStack<'a> {
    pub fn new<'b: 'a>(t1: &'a Ticket, t2: &'b Ticket) -> Self {
        TicketStack {
            ticket_ids: [&t1.id, &t2.id],
            price: t1.price + t2.price,
        }
    }
}

#[derive(Serialize)]
pub struct SearchResponse<'a> {
    pub solutions: Vec<TicketStack<'a>>,
}

impl<'a> SearchResponse<'a> {
    pub fn new(solutions: Vec<TicketStack<'a>>) -> Self {
        SearchResponse {
            solutions: solutions,
        }
    }
}

struct TicketServer {
    storage: Rc<RefCell<Vec<Ticket>>>,
}

impl TicketServer {
    pub fn new() -> Self {
        TicketServer {
            storage: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn storage(&self) -> Rc<RefCell<Vec<Ticket>>> {
        Rc::clone(&self.storage)
    }

    pub fn tickets(&self) -> impl Future<Item = Response, Error = hyper::Error> {
        let storage = self.storage.borrow();
        let body = serde_json::to_string(&*storage).unwrap();

        let resp = Response::new()
            .with_status(StatusCode::Ok)
            .with_header(header::ContentLength(body.len() as u64))
            .with_body(body);
        future::ok(resp)
    }

    pub fn batch_insert(&self, body: Body) -> impl Future<Item = Response, Error = hyper::Error> {
        let shared_storage = self.storage();
        body.concat2().and_then(move |body: Chunk| {
            // todo: deserialization error JSON response
            let mut insert_req: InsertRequest = match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(error) => {
                    let body = error.to_string();
                    let resp = Response::new()
                        .with_status(StatusCode::BadRequest)
                        .with_header(header::ContentLength(body.len() as u64))
                        .with_body(body);
                    return future::ok(resp)
                }
            };
            shared_storage.borrow_mut().append(&mut insert_req.tickets);
            let body = serde_json::to_string(&InsertResponse::new(Status::Success)).unwrap();
            let resp = Response::new()
                .with_status(StatusCode::Ok)
                .with_header(header::ContentLength(body.len() as u64))
                .with_body(body);
            future::ok(resp)
        })
    }

    pub fn search(&self, body: Body) -> impl Future<Item = Response, Error = hyper::Error> {
        let shared_storage = self.storage();
        body.concat2().and_then(move |body: Chunk| {
            // todo: deserialization error JSON response
            let search_request: SearchRequest = match serde_json::from_slice(&body) {
                Ok(req) => req,
                Err(error) => {
                    let body = error.to_string();
                    let resp = Response::new()
                        .with_status(StatusCode::BadRequest)
                        .with_header(header::ContentLength(body.len() as u64))
                        .with_body(body);
                    return future::ok(resp)
                }
            };
            let storage = shared_storage.borrow();

            let mut res = Vec::new();
            {
                let mut departure_match: Vec<&Ticket> = Vec::new();
                let mut arrival_map = MultiMap::new();
                for ticket in &*storage {
                    if search_request.departure_code == ticket.departure_code
                        && search_request.arrival_code != ticket.arrival_code
                        && search_request.departure_time_start <= ticket.departure_time
                        && ticket.departure_time <= search_request.departure_time_end
                    {
                        departure_match.push(ticket);
                    } else if search_request.departure_code != ticket.departure_code
                        && search_request.arrival_code == ticket.arrival_code
                    {
                        let dep_code = ticket.departure_code.clone();
                        arrival_map.insert(dep_code, ticket);
                    }
                }

                for t1 in &departure_match {
                    if let Some(t2_match) = arrival_map.get_vec(&t1.arrival_code) {
                        let iter = t2_match.iter().filter(|t2| {
                            t1.arrival_time + DELTA_MIN < t2.departure_time
                                && t2.departure_time < t1.arrival_time + DELTA_MAX
                        });
                        for t2 in iter {
                            res.push(TicketStack::new(t1, t2))
                        }
                    }
                }
            }

            let body = serde_json::to_string(&SearchResponse::new(res)).unwrap();

            let resp = Response::new()
                .with_status(StatusCode::Ok)
                .with_header(header::ContentLength(body.len() as u64))
                .with_body(body);
            future::ok(resp)
        })
    }
}

impl Service for TicketServer {
    type Request = Request;
    type Response = Response;

    type Error = hyper::Error;

    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        match (req.method(), req.path()) {
            (&Method::Get, "/tickets") => {
                Box::new(self.tickets())
            }
            (&Method::Post, "/batch_insert") => {
                let fut = self.batch_insert(req.body());
                Box::new(fut)
            }
            (&Method::Post, "/search") => {
                let fut = self.search(req.body());
                Box::new(fut)
            }
            (_, _) => {
                let resp = Response::new().with_status(StatusCode::NotFound);
                Box::new(future::ok(resp))
            }
        }
    }
}

fn main() {
    let addr = "127.0.0.1:3000".parse().unwrap();
    let service = Rc::new(TicketServer::new());
    let server = Http::new()
        .bind(&addr, move || Ok(Rc::clone(&service)))
        .unwrap();
    server.run().unwrap();
}

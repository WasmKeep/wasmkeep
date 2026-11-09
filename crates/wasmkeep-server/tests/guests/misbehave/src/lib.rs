//! Test guest. `/spin` never returns, `/panic` traps, anything else answers
//! `ok`. Used to prove that a bad guest cannot take the server down.

use std::io::Write as _;

use wasip2::http::types::{
    Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

wasip2::http::proxy::export!(Misbehave);

struct Misbehave;

impl wasip2::exports::http::incoming_handler::Guest for Misbehave {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        match request.path_with_query().as_deref() {
            Some("/spin") => {
                let mut n: u64 = 0;
                loop {
                    n = std::hint::black_box(n.wrapping_add(1));
                }
            }
            Some("/panic") => panic!("misbehave: requested panic"),
            _ => {}
        }
        let response = OutgoingResponse::new(Fields::new());
        let body = response.body().unwrap();
        ResponseOutparam::set(response_out, Ok(response));
        let mut out = body.write().unwrap();
        out.write_all(b"ok\n").unwrap();
        out.flush().unwrap();
        drop(out);
        OutgoingBody::finish(body, None).unwrap();
    }
}

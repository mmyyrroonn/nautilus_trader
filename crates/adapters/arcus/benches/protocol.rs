// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use nautilus_arcus::{
    models::*,
    signing::{ArcusSigner, order_signing_payload},
};
use rust_decimal_macros::dec;

fn protocol(c: &mut Criterion) {
    let fixture = include_str!("../test_data/market.json");
    let market: ArcusMarket = serde_json::from_str(fixture).unwrap();
    let order = ArcusOrderRequest {
        address: "0xabcdef0123456789abcdef0123456789abcdef01".into(),
        account_index: 0,
        market_id: 1,
        order_side: ArcusOrderSide::Buy,
        order_type: ArcusOrderType::Limit,
        time_in_force: ArcusTimeInForce::Alo,
        quantity: dec!(0.001),
        price: dec!(50000.1),
        good_til_time: "1800000000000000".into(),
        reduce_only: false,
        client_id: Some("benchmark-order".into()),
        timestamp: 1790000000000000000,
    };
    let signer = ArcusSigner::from_seed_hex(&"00".repeat(32)).unwrap();
    let modify = ArcusModifyRequest {
        address: order.address.clone(),
        account_index: order.account_index,
        market_id: order.market_id,
        order_id: "abcdef0123456789".into(),
        quantity: order.quantity,
        price: order.price,
        side: order.order_side,
        time_in_force: order.time_in_force,
        reduce_only: order.reduce_only,
        good_til_time: order.good_til_time.clone(),
        client_time: order.timestamp.to_string(),
    };
    c.bench_function("arcus_market_decode", |b| {
        b.iter(|| serde_json::from_str::<ArcusMarket>(black_box(fixture)).unwrap());
    });
    c.bench_function("arcus_order_integer_payload", |b| {
        b.iter(|| {
            order_signing_payload(black_box(&order), black_box(&market), order.timestamp).unwrap()
        });
    });
    c.bench_function("arcus_order_sign", |b| {
        b.iter(|| {
            signer
                .sign_order(black_box(&order), black_box(&market), order.timestamp)
                .unwrap()
        });
    });
    c.bench_function("arcus_modify_sign", |b| {
        b.iter(|| {
            signer
                .sign_modify(black_box(&modify), black_box(&market), order.timestamp)
                .unwrap()
        });
    });
}

criterion_group!(benches, protocol);
criterion_main!(benches);

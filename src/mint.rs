use std::sync::Arc;

use ethers::prelude::*;

use crate::arc_pay_contract;

pub(crate) async fn mint(
    events: Event<Arc<Provider<Http>>, Provider<Http>, arc_pay_contract::MintFilter>,
) {
    let mut stream = events.stream().await.unwrap();

    while let Some(Ok(f)) = stream.next().await {
        dbg!(f);
    }
}

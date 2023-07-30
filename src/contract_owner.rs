use std::sync::Arc;

use ethers::{
    prelude::{Address, Http, LocalWallet, Provider, SignerMiddleware, U256},
    signers::Signer,
};

use crate::{ArcPayContract, ARCPAY_ADDRESS};
use eyre::Result;

pub(crate) struct ContractOwner {
    contract: ArcPayContract<SignerMiddleware<Arc<Provider<Http>>, LocalWallet>>,
}

impl ContractOwner {
    pub(crate) async fn new() -> Result<Self> {
        let provider = Arc::new(Provider::<Http>::try_from("http://127.0.0.1:8545")?);
        let wallet: LocalWallet =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse::<LocalWallet>()
                .unwrap();
        let client = Arc::new(SignerMiddleware::new(
            provider,
            wallet.with_chain_id(31337u64),
        ));
        let contract_address = ARCPAY_ADDRESS.parse::<Address>()?;
        let contract = ArcPayContract::new(contract_address, client);

        Ok(ContractOwner { contract })
    }

    pub(crate) async fn update_state_root(&self, state_root: U256) -> Result<()> {
        let tx = self.contract.update_state(state_root);
        dbg!(&tx);
        let pending_tx = tx.send().await?;
        let _mined_tx = pending_tx.confirmations(3).await?;
        Ok(())
    }

    pub(crate) async fn get_state_root(&self) -> Result<U256> {
        Ok(self.contract.state_root().call().await?)
    }
}

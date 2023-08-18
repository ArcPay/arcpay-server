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
        let provider = Arc::new(Provider::<Http>::try_from("https://twilight-cosmological-shadow.ethereum-sepolia.discover.quiknode.pro/b324db2b9a4c678722d4d294d2e1e160d6840335/")?);
        let wallet: LocalWallet =
            "0x496d9e930e4a133fc73b71314f8f7305be5d52eeb161f45e822bf07764b1a4be"
                .parse::<LocalWallet>()
                .unwrap();
        let client = Arc::new(SignerMiddleware::new(
            provider,
            wallet.with_chain_id(11155111u64),
        ));
        let contract_address = ARCPAY_ADDRESS.parse::<Address>()?;
        let contract = ArcPayContract::new(contract_address, client);

        Ok(ContractOwner { contract })
    }

    pub(crate) async fn update_state_root(&self, state_root: U256, mint_time: U256) -> Result<()> {
        let tx = self.contract.update_state(state_root, mint_time);
        let pending_tx = tx.send().await?;
        let _mined_tx = pending_tx.await?;
        // TODO change to below before launch. This waits for 3 blocks.
        // This slows down manual testing.
        // let _mined_tx = pending_tx.confirmations(3).await?;
        Ok(())
    }

    pub(crate) async fn get_state_root(&self) -> Result<U256> {
        Ok(self.contract.state_root().call().await?)
    }
}

use std::mem::size_of;

use anchor_lang::Discriminator;
use log::{debug, info};
use marginfi::state::{
    marginfi_account::MarginfiAccount,
    marginfi_group::{Bank, MarginfiGroup},
};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use crate::{comms::CommsClient, config::Config};
use anyhow::{anyhow, Result};

const ADDRESSES_CHUNK_SIZE: usize = 100;
const ANCHOR_DISCRIMINATOR_LEN: usize = 8;
const PUBKEY_BYTES: usize = 32;
const MARGINFI_GROUP_DATA_LEN: usize = ANCHOR_DISCRIMINATOR_LEN + size_of::<MarginfiGroup>();
const MARGINFI_BANK_DATA_LEN: usize = ANCHOR_DISCRIMINATOR_LEN + size_of::<Bank>();
const MARGINFI_ACCOUNT_DATA_LEN: usize = ANCHOR_DISCRIMINATOR_LEN + size_of::<MarginfiAccount>();
const MARGINFI_ACCOUNT_GROUP_OFFSET: usize = ANCHOR_DISCRIMINATOR_LEN;
const MARGINFI_ACCOUNT_AUTHORITY_OFFSET: usize = ANCHOR_DISCRIMINATOR_LEN + PUBKEY_BYTES;

pub struct RpcCommsClient {
    solana_rpc_client: RpcClient,
}

impl CommsClient for RpcCommsClient {
    fn new(config: &Config) -> Result<Self> {
        let solana_rpc_client =
            RpcClient::new_with_commitment(&config.rpc_url, CommitmentConfig::confirmed());
        Ok(RpcCommsClient { solana_rpc_client })
    }

    fn get_account(&self, pubkey: &Pubkey) -> Result<Account> {
        self.solana_rpc_client
            .get_account(pubkey)
            .map_err(|e| anyhow!("Failed to get account {}: {}", pubkey, e))
    }

    fn get_program_accounts(&self, program_id: &Pubkey) -> Result<Vec<(Pubkey, Account)>> {
        let mut accounts = Vec::new();

        info!("Fetching Marginfi groups...");
        let mut groups =
            self.get_program_accounts_for_type(program_id, MarginfiProgramAccountType::Group)?;
        info!("Fetched {} Marginfi groups", groups.len());
        let group_pubkeys: Vec<Pubkey> = groups.iter().map(|(pubkey, _)| *pubkey).collect();
        accounts.append(&mut groups);

        info!("Fetching Marginfi banks...");
        let mut banks =
            self.get_program_accounts_for_type(program_id, MarginfiProgramAccountType::Bank)?;
        info!("Fetched {} Marginfi banks", banks.len());
        accounts.append(&mut banks);

        info!(
            "Fetching Marginfi accounts for {} groups",
            group_pubkeys.len()
        );
        let mut marginfi_accounts =
            self.get_marginfi_accounts_by_group(program_id, &group_pubkeys)?;
        info!("Fetched {} Marginfi accounts", marginfi_accounts.len());
        accounts.append(&mut marginfi_accounts);

        Ok(accounts)
    }

    fn get_accounts(&self, addresses: &[Pubkey]) -> Result<Vec<(Pubkey, Account)>> {
        let mut tuples: Vec<(Pubkey, Account)> = Vec::new();

        for chunk in addresses.chunks(ADDRESSES_CHUNK_SIZE) {
            let accounts = self.solana_rpc_client.get_multiple_accounts(chunk)?;
            for (address, account_opt) in chunk.iter().zip(accounts.iter()) {
                if let Some(account) = account_opt {
                    tuples.push((*address, account.clone()));
                }
            }
        }

        Ok(tuples)
    }
}

impl RpcCommsClient {
    fn get_program_accounts_for_type(
        &self,
        program_id: &Pubkey,
        account_kind: MarginfiProgramAccountType,
    ) -> Result<Vec<(Pubkey, Account)>> {
        let filters = account_kind.filters();
        self.get_program_accounts_with_filters(program_id, filters, account_kind)
    }

    fn get_program_accounts_with_filters(
        &self,
        program_id: &Pubkey,
        filters: Vec<RpcFilterType>,
        account_kind: MarginfiProgramAccountType,
    ) -> Result<Vec<(Pubkey, Account)>> {
        let filter_summary = Self::summarize_filters(&filters);
        debug!(
            "Querying {} accounts with filters: {}",
            account_kind.as_str(),
            filter_summary
        );

        let config = RpcProgramAccountsConfig {
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            filters: Some(filters),
            with_context: None,
            sort_results: None,
        };

        self.solana_rpc_client
            .get_program_accounts_with_config(program_id, config)
            .map_err(|e| {
                anyhow!(
                    "Failed to get {} accounts for program {}: {}",
                    account_kind.as_str(),
                    program_id,
                    e
                )
            })
            .map(|accounts| {
                debug!(
                    "Fetched {} {} accounts (filters: {})",
                    accounts.len(),
                    account_kind.as_str(),
                    filter_summary
                );
                accounts
            })
    }

    fn get_marginfi_accounts_by_group(
        &self,
        program_id: &Pubkey,
        group_pubkeys: &[Pubkey],
    ) -> Result<Vec<(Pubkey, Account)>> {
        if group_pubkeys.is_empty() {
            return self.get_program_accounts_for_type(
                program_id,
                MarginfiProgramAccountType::MarginfiAccount,
            );
        }

        let mut accounts = Vec::new();
        for group_pubkey in group_pubkeys {
            let mut group_accounts =
                self.fetch_marginfi_accounts_for_prefix(program_id, *group_pubkey, Vec::new())?;
            accounts.append(&mut group_accounts);
        }

        Ok(accounts)
    }

    fn fetch_marginfi_accounts_for_prefix(
        &self,
        program_id: &Pubkey,
        group_pubkey: Pubkey,
        authority_prefix: Vec<u8>,
    ) -> Result<Vec<(Pubkey, Account)>> {
        let mut filters = MarginfiProgramAccountType::MarginfiAccount.filters();
        filters.push(RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            MARGINFI_ACCOUNT_GROUP_OFFSET,
            group_pubkey.to_bytes().to_vec(),
        )));
        if !authority_prefix.is_empty() {
            filters.push(RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                MARGINFI_ACCOUNT_AUTHORITY_OFFSET,
                authority_prefix.clone(),
            )));
        }

        if authority_prefix.is_empty() {
            info!("Fetching Marginfi accounts for group {}", group_pubkey);
        } else {
            debug!(
                "Fetching Marginfi accounts for group {} prefix {}",
                group_pubkey,
                Self::format_prefix(&authority_prefix)
            );
        }

        match self.get_program_accounts_with_filters(
            program_id,
            filters,
            MarginfiProgramAccountType::MarginfiAccount,
        ) {
            Ok(accounts) => Ok(accounts),
            Err(err) if Self::is_scan_limit_error(&err) => {
                info!(
                    "Scan limit hit for group {} prefix {}. Splitting further...",
                    group_pubkey,
                    Self::format_prefix(&authority_prefix)
                );
                if authority_prefix.len() >= PUBKEY_BYTES {
                    return Err(err);
                }

                let mut chunked_accounts = Vec::new();
                for byte in 0u8..=u8::MAX {
                    let mut next_prefix = authority_prefix.clone();
                    next_prefix.push(byte);
                    let mut accounts = self.fetch_marginfi_accounts_for_prefix(
                        program_id,
                        group_pubkey,
                        next_prefix,
                    )?;
                    chunked_accounts.append(&mut accounts);
                }

                Ok(chunked_accounts)
            }
            Err(err) => Err(err),
        }
    }

    fn is_scan_limit_error(err: &anyhow::Error) -> bool {
        err.to_string()
            .contains("scan aborted: The accumulated scan results exceeded the limit")
    }

    fn summarize_filters(filters: &[RpcFilterType]) -> String {
        filters
            .iter()
            .map(|filter| match filter {
                RpcFilterType::DataSize(size) => format!("data_size={}", size),
                RpcFilterType::Memcmp(memcmp) => format!(
                    "memcmp@{}:len={}",
                    memcmp.offset(),
                    Self::memcmp_byte_len(memcmp)
                ),
                RpcFilterType::TokenAccountState => "token_account_state".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn memcmp_byte_len(memcmp: &Memcmp) -> usize {
        memcmp.bytes().map(|bytes| bytes.len()).unwrap_or_default()
    }

    fn format_prefix(prefix: &[u8]) -> String {
        if prefix.is_empty() {
            "<full>".to_string()
        } else {
            prefix
                .iter()
                .map(|byte| format!("{:02X}", byte))
                .collect::<Vec<_>>()
                .join("")
        }
    }
}

#[derive(Clone, Copy)]
enum MarginfiProgramAccountType {
    Group,
    Bank,
    MarginfiAccount,
}

impl MarginfiProgramAccountType {
    fn filters(&self) -> Vec<RpcFilterType> {
        vec![
            RpcFilterType::DataSize(self.data_size()),
            RpcFilterType::Memcmp(Memcmp::new_raw_bytes(0, self.discriminator().to_vec())),
        ]
    }

    fn data_size(&self) -> u64 {
        match self {
            Self::Group => MARGINFI_GROUP_DATA_LEN as u64,
            Self::Bank => MARGINFI_BANK_DATA_LEN as u64,
            Self::MarginfiAccount => MARGINFI_ACCOUNT_DATA_LEN as u64,
        }
    }

    fn discriminator(&self) -> &'static [u8] {
        match self {
            Self::Group => <MarginfiGroup as Discriminator>::DISCRIMINATOR,
            Self::Bank => <Bank as Discriminator>::DISCRIMINATOR,
            Self::MarginfiAccount => <MarginfiAccount as Discriminator>::DISCRIMINATOR,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Group => "MarginfiGroup",
            Self::Bank => "Bank",
            Self::MarginfiAccount => "MarginfiAccount",
        }
    }
}

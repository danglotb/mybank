use std::{
    error::Error,
    fmt::{Display, Formatter},
};

use async_trait::async_trait;
use cqrs_es::{
    mem_store::MemStore, Aggregate, AggregateError, DomainEvent, EventEnvelope, Query, View,
};
use postgres_es::{default_postgress_pool, PostgresEventRepository, PostgresViewRepository};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Postgres};

#[derive(Debug, Deserialize)]
pub enum BankAccountCommand {
    OpenAccount { account_id: String },
    DepositMoney { amount: f64 },
    WithdrawMoney { amount: f64 },
    WriteCheck { check_number: String, amount: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BankAccountEvent {
    AccountOpened {
        account_id: String,
    },
    CustomerDepositedMoney {
        amount: f64,
        balance: f64,
    },
    CustomerWithdrewCash {
        amount: f64,
        balance: f64,
    },
    CustomerWroteCheck {
        check_number: String,
        amount: f64,
        balance: f64,
    },
}

impl DomainEvent for BankAccountEvent {
    fn event_type(&self) -> String {
        let event_type: &str = match self {
            BankAccountEvent::AccountOpened { .. } => "AccountOpened",
            BankAccountEvent::CustomerDepositedMoney { .. } => "CustomerDepositedMoney",
            BankAccountEvent::CustomerWithdrewCash { .. } => "CustomerWithdrewCash",
            BankAccountEvent::CustomerWroteCheck { .. } => "CustomerWroteCheck",
        };
        event_type.to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, PartialEq)]
pub struct BankAccountError(String);

impl Display for BankAccountError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BankAccountError {}

impl From<&str> for BankAccountError {
    fn from(message: &str) -> Self {
        BankAccountError(message.to_string())
    }
}

pub struct BankAccountServices;

impl BankAccountServices {
    async fn atm_withdrawal(&self, atm_id: &str, amount: f64) -> Result<(), AtmError> {
        Ok(())
    }

    async fn validate_check(&self, account: &str, check: &str) -> Result<(), CheckingError> {
        Ok(())
    }
}

pub struct AtmError;
pub struct CheckingError;

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct BankAccount {
    opened: bool,
    // this is a floating point for our example, don't do this IRL
    balance: f64,
}

#[async_trait]
impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Event = BankAccountEvent;
    type Error = BankAccountError;
    type Services = BankAccountServices;

    // This identifier should be unique to the system.
    fn aggregate_type() -> String {
        "Account".to_string()
    }

    // The aggregate logic goes here. Note that this will be the _bulk_ of a CQRS system
    // so expect to use helper functions elsewhere to keep the code clean.
    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::DepositMoney { amount } => {
                let balance = self.balance + amount;
                Ok(vec![BankAccountEvent::CustomerDepositedMoney {
                    amount,
                    balance,
                }])
            }
            BankAccountCommand::WithdrawMoney { amount } => {
                let balance = self.balance - amount;
                if balance < 0_f64 {
                    return Err(BankAccountError(String::from("funds not available")));
                }
                Ok(vec![BankAccountEvent::CustomerWithdrewCash {
                    amount,
                    balance,
                }])
            }
            BankAccountCommand::WriteCheck {
                check_number,
                amount,
            } => {
                let balance = self.balance - amount;
                if balance < 0_f64 {
                    return Err(BankAccountError(String::from("funds not available")));
                }
                Ok(vec![BankAccountEvent::CustomerWroteCheck {
                    check_number,
                    amount,
                    balance,
                }])
            }
            _ => Ok(vec![]),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { .. } => self.opened = true,

            BankAccountEvent::CustomerDepositedMoney { amount: _, balance } => {
                self.balance = balance;
            }

            BankAccountEvent::CustomerWithdrewCash { amount: _, balance } => {
                self.balance = balance;
            }

            BankAccountEvent::CustomerWroteCheck {
                check_number: _,
                amount: _,
                balance,
            } => {
                self.balance = balance;
            }
        }
    }
}

struct SimpleLoggingQuery {}

#[async_trait]
impl Query<BankAccount> for SimpleLoggingQuery {
    async fn dispatch(&self, aggregate_id: &str, events: &[EventEnvelope<BankAccount>]) {
        for event in events {
            println!("{}-{}\n{:#?}", aggregate_id, event.sequence, &event.payload);
        }
    }
}

async fn configure_repo() -> PostgresEventRepository {
    let connection_string = "postgresql://postgres:password@localhost:5432/test";
    let pool: Pool<Postgres> = default_postgress_pool(connection_string).await;
    PostgresEventRepository::new(pool)
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct BankAccountView {}

impl View<BankAccount> for BankAccount {
    fn update(&mut self, event: &EventEnvelope<BankAccount>) {
        match &event.payload {
            BankAccountEvent::CustomerDepositedMoney { amount, balance } => {
                // self.ledger.push(LedgerEntry::new("deposit", *amount));
                self.balance = *balance;
            }
            BankAccountEvent::AccountOpened { account_id } => todo!(),
            BankAccountEvent::CustomerWithdrewCash { amount, balance } => todo!(),
            BankAccountEvent::CustomerWroteCheck {
                check_number,
                amount,
                balance,
            } => todo!(),
        }
    }
}

type MyViewRepository = PostgresViewRepository<BankAccount, BankAccount>;

fn configure_view_repository(db_pool: Pool<Postgres>) -> MyViewRepository {
    PostgresViewRepository::new("my_view_name", db_pool)
}

fn main() {
    let event_store = MemStore::<BankAccount>::default();
}

#[cfg(test)]
mod aggregate_tests {
    use super::*;
    use cqrs_es::{test::TestFramework, CqrsFramework};

    type AccountTestFramework = TestFramework<BankAccount>;

    #[tokio::test]
    async fn test_event_store() {
        let event_store = MemStore::<BankAccount>::default();
        let query = SimpleLoggingQuery {};
        let cqrs = CqrsFramework::new(event_store, vec![Box::new(query)], BankAccountServices);

        {
            let aggregate_id = "aggregate-instance-Z";

            // deposit $1000
            cqrs.execute(
                aggregate_id,
                BankAccountCommand::DepositMoney { amount: 1000_f64 },
            )
            .await
            .unwrap();

            // write a check for $236.15
            cqrs.execute(
                aggregate_id,
                BankAccountCommand::WriteCheck {
                    check_number: "1337".to_string(),
                    amount: 236.15,
                },
            )
            .await
            .unwrap();
        }

        {
            let aggregate_id = "aggregate-instance-B";

            // deposit $1000
            cqrs.execute(
                aggregate_id,
                BankAccountCommand::DepositMoney { amount: 1000_f64 },
            )
            .await
            .unwrap();

            // write a check for $236.15
            cqrs.execute(
                aggregate_id,
                BankAccountCommand::WriteCheck {
                    check_number: "1337".to_string(),
                    amount: 236.15,
                },
            )
            .await
            .unwrap();
        }

        {
            let aggregate_id = "aggregate-instance-Z";

            // deposit $1000
            cqrs.execute(
                aggregate_id,
                BankAccountCommand::DepositMoney { amount: 1000_f64 },
            )
            .await
            .unwrap();

            // write a check for $236.15
            cqrs.execute(
                aggregate_id,
                BankAccountCommand::WriteCheck {
                    check_number: "1337".to_string(),
                    amount: 236.15,
                },
            )
            .await
            .unwrap();
        }
    }

    #[test]

    fn test_deposit_money() {
        let expected = BankAccountEvent::CustomerDepositedMoney {
            amount: 200.0,
            balance: 200.0,
        };

        AccountTestFramework::with(BankAccountServices)
            .given_no_previous_events()
            .when(BankAccountCommand::DepositMoney { amount: 200.0 })
            .then_expect_events(vec![expected]);
    }

    #[test]
    fn test_deposit_money_with_balance() {
        let previous = BankAccountEvent::CustomerDepositedMoney {
            amount: 200.0,
            balance: 200.0,
        };
        let expected = BankAccountEvent::CustomerDepositedMoney {
            amount: 200.0,
            balance: 400.0,
        };

        AccountTestFramework::with(BankAccountServices)
            .given(vec![previous])
            .when(BankAccountCommand::DepositMoney { amount: 200.0 })
            .then_expect_events(vec![expected]);
    }
    #[test]
    fn test_withdraw_money() {
        let previous = BankAccountEvent::CustomerDepositedMoney {
            amount: 200.0,
            balance: 200.0,
        };
        let expected = BankAccountEvent::CustomerWithdrewCash {
            amount: 100.0,
            balance: 100.0,
        };

        AccountTestFramework::with(BankAccountServices)
            .given(vec![previous])
            .when(BankAccountCommand::WithdrawMoney { amount: 100.0 })
            .then_expect_events(vec![expected]);
    }

    #[test]
    fn test_withdraw_money_funds_not_available() {
        AccountTestFramework::with(BankAccountServices)
            .given_no_previous_events()
            .when(BankAccountCommand::WithdrawMoney { amount: 200.0 })
            .then_expect_error(BankAccountError("funds not available".to_string()));
    }
}

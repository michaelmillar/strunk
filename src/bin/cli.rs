use std::time::Duration;

use clap::{Parser, Subcommand};
use strunk::config::StrunkConfig;
use strunk::Strunk;
use tabled::{Table, Tabled};

#[derive(Parser)]
#[command(name = "strunk", about = "Omit needless infrastructure.")]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Migrate,

    Stats {
        #[arg(long)]
        queue: Option<String>,
    },

    DeadLetters {
        queue: String,
        #[arg(long, default_value = "25")]
        limit: i64,
    },

    Retry {
        task_id: i64,
    },

    RetryAll {
        queue: String,
    },

    Purge {
        queue: String,
        #[arg(long)]
        status: Option<String>,
    },

    Subscribers,

    Lag {
        subscriber_id: String,
    },

    Health,
}

#[derive(Tabled)]
struct QueueRow {
    queue: String,
    pending: i64,
    claimed: i64,
    delivered: i64,
    dead: i64,
    oldest_pending: String,
}

#[derive(Tabled)]
struct DeadLetterRow {
    id: i64,
    queue: String,
    attempts: i32,
    created: String,
    payload_preview: String,
}

#[derive(Tabled)]
struct SubscriberRow {
    id: String,
    entity_type: String,
    last_seen_id: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let config = StrunkConfig {
        database_url: cli.database_url,
        ..Default::default()
    };

    let strunk = Strunk::new(config).await?;

    match cli.command {
        Command::Migrate => {
            strunk.migrate().await?;
            println!("migration complete");
        }

        Command::Stats { queue } => {
            if let Some(queue) = queue {
                let s = strunk.queue_stats(&queue).await?;
                let rows = vec![QueueRow {
                    queue: s.queue,
                    pending: s.pending,
                    claimed: s.claimed,
                    delivered: s.delivered,
                    dead: s.dead,
                    oldest_pending: s
                        .oldest_pending
                        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_default(),
                }];
                println!("{}", Table::new(rows));
            } else {
                let s = strunk.overall_stats().await?;
                println!("pending:   {}", s.total_pending);
                println!("claimed:   {}", s.total_claimed);
                println!("delivered: {}", s.total_delivered);
                println!("dead:      {}", s.total_dead);
                println!("total:     {}", s.table_size);
            }
        }

        Command::DeadLetters { queue, limit } => {
            let dead = strunk.dead_letters(&queue, limit).await?;
            if dead.is_empty() {
                println!("no dead letters in '{}'", queue);
            } else {
                let rows: Vec<DeadLetterRow> = dead
                    .into_iter()
                    .map(|d| {
                        let preview = d.payload.to_string();
                        let preview = if preview.len() > 80 {
                            format!("{}...", &preview[..77])
                        } else {
                            preview
                        };
                        DeadLetterRow {
                            id: d.id,
                            queue: d.key.clone(),
                            attempts: d.attempts,
                            created: d.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                            payload_preview: preview,
                        }
                    })
                    .collect();
                println!("{}", Table::new(rows));
            }
        }

        Command::Retry { task_id } => {
            strunk.retry_dead(task_id).await?;
            println!("task {} moved back to pending", task_id);
        }

        Command::RetryAll { queue } => {
            let dead = strunk.dead_letters(&queue, 10_000).await?;
            let count = dead.len();
            for d in dead {
                strunk.retry_dead(d.id).await?;
            }
            println!("retried {} dead letters from '{}'", count, queue);
        }

        Command::Purge { queue, status } => {
            let status_filter = status.as_deref().unwrap_or("delivered");
            let result = sqlx::query(
                "DELETE FROM strunk_outbox WHERE key = $1 AND status = $2",
            )
            .bind(&queue)
            .bind(status_filter)
            .execute(strunk.pool())
            .await?;
            println!(
                "purged {} '{}' rows from '{}'",
                result.rows_affected(),
                status_filter,
                queue
            );
        }

        Command::Subscribers => {
            let rows = sqlx::query_as::<_, (String, String, i64)>(
                "SELECT id, entity_type, last_seen_id FROM strunk_subscribers ORDER BY id",
            )
            .fetch_all(strunk.pool())
            .await?;

            if rows.is_empty() {
                println!("no subscribers registered");
            } else {
                let table_rows: Vec<SubscriberRow> = rows
                    .into_iter()
                    .map(|(id, entity_type, last_seen_id)| SubscriberRow {
                        id,
                        entity_type,
                        last_seen_id,
                    })
                    .collect();
                println!("{}", Table::new(table_rows));
            }
        }

        Command::Lag { subscriber_id } => {
            match strunk.subscriber_stats(&subscriber_id).await? {
                Some(s) => {
                    println!("subscriber:  {}", s.id);
                    println!("entity type: {}", s.entity_type);
                    println!("last seen:   {}", s.last_seen_id);
                    println!("latest:      {}", s.latest_outbox_id);
                    println!("lag:         {}", s.lag);
                }
                None => {
                    println!("subscriber '{}' not found", subscriber_id);
                }
            }
        }

        Command::Health => {
            let health = strunk.health(Duration::from_secs(300)).await;
            match health {
                Ok(h) => {
                    println!("database:    ok");
                    println!("pending:     {}", h.pending);
                    println!("oldest (s):  {}", h.oldest_pending_age_secs.unwrap_or(0));
                    if h.healthy {
                        println!("status:      healthy");
                    } else {
                        println!("status:      unhealthy (stale pending rows)");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    println!("database:    unreachable ({})", e);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

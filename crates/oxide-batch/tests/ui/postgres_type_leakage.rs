//! The M1 repository ports do not expose or re-export PostgreSQL driver types.

use oxide_batch::sqlx::PgPool;

fn main() {
    let _: Option<PgPool> = None;
}

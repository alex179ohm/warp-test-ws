// #![deny(warnings)]
use std::collections::HashMap;
// needed for tokio_postgres::Config::from_str
use std::env;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::{FutureExt, StreamExt};
use tokio::sync::{mpsc, Mutex};
//use warp::filters::{cookie, BoxedFilter};
use warp::ws::{Message, WebSocket};
use warp::Filter;

// Notice: prefer re-exported
// use mobc::Pool;
// use tokio_postgres::Config;
// use tokio_postgres::NoTls;

use mobc_postgres::mobc::{Connection, Pool};
use mobc_postgres::tokio_postgres::Config;
use mobc_postgres::tokio_postgres::NoTls;
use mobc_postgres::PgConnectionManager;

/// Our global unique user id counter.
static NEXT_USER_ID: AtomicUsize = AtomicUsize::new(1);

/// Our state of currently connected users.
///
/// - Key is their id
/// - Value is a sender of `warp::ws::Message`
type Users = Arc<Mutex<HashMap<usize, mpsc::UnboundedSender<Result<Message, warp::Error>>>>>;

/// Db stuff
type DbConnManager = PgConnectionManager<NoTls>;
type DbPool = Pool<DbConnManager>;
type DbClient = Connection<DbConnManager>;

#[derive(Clone)]
struct AsteDbPool {
    pub db_pool: DbPool,
}

impl AsteDbPool {
    pub fn new() -> Self {
        // postgres://apprendo:apprendo@localhost:5433
        let db_url = env::var("DATABASE_URL").expect("Missing DATABASE_URL env");
        let config = Config::from_str(&db_url).unwrap();
        let manager = PgConnectionManager::new(config, NoTls);
        AsteDbPool {
            db_pool: Pool::builder().max_open(20).build(manager),
        }
    }

    pub async fn get_conn(&self) -> Option<DbClient> {
        match self.db_pool.get().await {
            Ok(conn) => Some(conn),
            Err(_) => None,
        }
    }
}

// pub struct Session {
//     db: DbPool,
//     id: Option<u32>,
// }

// pub fn create_session_filter(db_url: &str) -> BoxedFilter<(Session,)> {
//     let pool = pg_pool(db_url);
//     warp::any()
//         .and(cookie::optional("EXAUTH"))
//         .and_then(move |key: Option<String>| {
//             let pool = pool.clone();
//             let key = key.as_ref().map(|s| &**s);
//             match pool.get() {
//                 Ok(conn) => Ok(Session::from_key(conn, key)),
//                 Err(e) => {
//                     error!("Failed to get a db connection");
//                     Err(custom(e))
//                 }
//             }
//         })
//         .boxed()
// }

#[tokio::main]
async fn main() {
    // Keep track of all connected users, key is usize, value
    // is a websocket sender.
    let users = Arc::new(Mutex::new(HashMap::new()));
    // Turn our "state" into a new Filter...
    let users = warp::any().map(move || users.clone());

    let pool = AsteDbPool::new();

    // let client = pool.get_conn().await.unwrap();
    // let db_client = warp::any().map(move || 42);
    // let db_client = warp::any().map(async move || pool.get_conn().await.unwrap());
    // let db_client = warp::any().map(async move { pool.get_conn().await.unwrap() });
    // let db_client = warp::any().map(|| async move { pool.get_conn().await.unwrap() });
    let db_client = warp::any().map(|| pool.clone().get_conn());

    // GET /chat -> websocket upgrade
    let chat = warp::path("chat")
        // The `ws()` filter will prepare Websocket handshake...
        .and(warp::ws())
        .and(users)
        .and(db_client)
        .map(|ws: warp::ws::Ws, users, _db_client| {
            // This will call our function if the handshake succeeds.
            ws.on_upgrade(move |socket| user_connected(socket, users))
        });

    // GET / -> index html
    let index = warp::path::end().map(|| warp::reply::html(INDEX_HTML));

    let routes = index.or(chat);

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

async fn user_connected(ws: WebSocket, users: Users) {
    // Use a counter to assign a new unique ID for this user.
    let my_id = NEXT_USER_ID.fetch_add(1, Ordering::Relaxed);

    eprintln!("new chat user: {}", my_id);

    // Split the socket into a sender and receive of messages.
    let (user_ws_tx, mut user_ws_rx) = ws.split();

    // Use an unbounded channel to handle buffering and flushing of messages
    // to the websocket...
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::task::spawn(rx.forward(user_ws_tx).map(|result| {
        if let Err(e) = result {
            eprintln!("websocket send error: {}", e);
        }
    }));

    // Save the sender in our list of connected users.
    users.lock().await.insert(my_id, tx);

    // Return a `Future` that is basically a state machine managing
    // this specific user's connection.

    // Make an extra clone to give to our disconnection handler...
    let users2 = users.clone();

    let pool = AsteDbPool::new();
    let client = pool.get_conn().await.unwrap();

    // Every time the user sends a message, broadcast it to
    // all other users...
    while let Some(result) = user_ws_rx.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("websocket error(uid={}): {}", my_id, e);
                break;
            }
        };
        let msg_c = msg.clone();
        let txt = if let Ok(s) = msg_c.to_str() { s } else { "" };
        let _my_id = my_id as u32;
        user_message(my_id, msg, &users).await;
        save_db(&client, _my_id, txt).await;
    }

    // user_ws_rx stream will keep processing as long as the user stays
    // connected. Once they disconnect, then...
    user_disconnected(my_id, &users2).await;
}

async fn save_db(client: &DbClient, my_id: u32, msg: &str) {
    let st = client
        .prepare_typed(
            "INSERT INTO asta (id, val) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET val=$2",
            &[
                mobc_postgres::tokio_postgres::types::Type::OID,
                mobc_postgres::tokio_postgres::types::Type::VARCHAR,
            ],
        )
        .await
        .expect("PREPARE statement failed");
    client
        .execute(&st, &[&my_id, &msg])
        .await
        .expect("EXECUTE statement failed");
}

async fn user_message(my_id: usize, msg: Message, users: &Users) {
    // Skip any non-Text messages...
    let msg = if let Ok(s) = msg.to_str() {
        s
    } else {
        return;
    };

    let new_msg = format!("<User#{}>: {}", my_id, msg);

    // New message from this user, send it to everyone else (except same uid)...
    //
    // We use `retain` instead of a for loop so that we can reap any user that
    // appears to have disconnected.
    for (&uid, tx) in users.lock().await.iter_mut() {
        if my_id != uid {
            if let Err(_disconnected) = tx.send(Ok(Message::text(new_msg.clone()))) {
                // The tx is disconnected, our `user_disconnected` code
                // should be happening in another task, nothing more to
                // do here.
            }
        }
    }
}

async fn user_disconnected(my_id: usize, users: &Users) {
    eprintln!("good bye user: {}", my_id);

    // Stream closed up, so remove from the user list
    users.lock().await.remove(&my_id);
}

static INDEX_HTML: &str = r#"
<!DOCTYPE html>
<html>
    <head>
        <title>Warp Chat</title>
    </head>
    <body>
        <h1>warp chat</h1>
        <div id="chat">
            <p><em>Connecting...</em></p>
        </div>
        <input type="text" id="text" />
        <button type="button" id="send">Send</button>
        <script type="text/javascript">
        var uri = 'ws://' + location.host + '/chat';
        var ws = new WebSocket(uri);

        function message(data) {
            var line = document.createElement('p');
            line.innerText = data;
            chat.appendChild(line);
        }

        ws.onopen = function() {
            chat.innerHTML = "<p><em>Connected!</em></p>";
        }

        ws.onmessage = function(msg) {
            message(msg.data);
        };

        text.onkeypress = function(evt) {
            if (evt.key !== 'Enter')
               return;
            var msg = text.value;
            ws.send(msg);
            text.value = '';

            message('<You>: ' + msg);
        };
        </script>
    </body>
</html>
"#;

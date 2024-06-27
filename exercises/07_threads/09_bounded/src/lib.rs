use std::fmt::Debug;
// TODO: Convert the implementation to use bounded channels.
use crate::data::{Ticket, TicketDraft};
use crate::store::{TicketId, TicketStore};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

pub mod data;
pub mod store;

#[derive(Clone)]
pub struct TicketStoreClient {
    capacity: usize,
    sender: SyncSender<Command>,
}

impl TicketStoreClient {
    pub fn insert(&self, draft: TicketDraft) -> Result<TicketId, TicketStoreClientError> {
        let (response_sender, response_receiver) = sync_channel(self.capacity);
        let command = Command::Insert {
            draft,
            response_channel: response_sender,
        };
        self.sender
            .try_send(command)
            .map_err(|err| TicketStoreClientError(err.to_string()))?;
        Ok(response_receiver
            .recv()
            .expect("Failed to receive response"))
    }

    pub fn get(&self, id: TicketId) -> Result<Option<Ticket>, TicketStoreClientError> {
        let (response_sender, response_receiver) = sync_channel(self.capacity);
        let command = Command::Get {
            id,
            response_channel: response_sender,
        };
        self.sender
            .try_send(command)
            .map_err(|err| TicketStoreClientError(err.to_string()))?;
        Ok(response_receiver
            .recv()
            .expect("Failed to receive response"))
    }
}

#[derive(Debug)]
pub struct TicketStoreClientError(String);

pub fn launch(capacity: usize) -> TicketStoreClient {
    let (sender, receiver) = sync_channel(capacity);
    std::thread::spawn(move || server(receiver));
    TicketStoreClient { capacity, sender }
}

enum Command {
    Insert {
        draft: TicketDraft,
        response_channel: SyncSender<TicketId>,
    },
    Get {
        id: TicketId,
        response_channel: SyncSender<Option<Ticket>>,
    },
}

pub fn server(receiver: Receiver<Command>) {
    let mut store = TicketStore::new();
    loop {
        match receiver.recv() {
            Ok(Command::Insert {
                draft,
                response_channel,
            }) => {
                let id = store.add_ticket(draft);
                response_channel.send(id).unwrap();
            }
            Ok(Command::Get {
                id,
                response_channel,
            }) => {
                let ticket = store.get(id);
                response_channel.send(ticket.cloned()).unwrap();
            }
            Err(_) => {
                // There are no more senders, so we can safely break
                // and shut down the server.
                break;
            }
        }
    }
}

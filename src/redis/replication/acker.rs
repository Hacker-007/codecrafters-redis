use tokio::sync::broadcast;

pub struct Acker {
    acked_bytes: usize,
    ack_sender: Option<broadcast::Sender<usize>>,
}

impl Acker {
    pub fn new(acked_bytes: usize) -> Self {
        Self {
            acked_bytes,
            ack_sender: None,
        }
    }

    pub fn get_bytes(&self) -> usize {
        self.acked_bytes
    }

    pub fn subscribe(&mut self) -> broadcast::Receiver<usize> {
        match self.ack_sender {
            Some(ref tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel(32);
                self.ack_sender.replace(tx);
                rx
            }
        }
    }

    pub fn ack(&mut self, bytes: usize) {
        self.acked_bytes = bytes;
        if let Some(ref sender) = self.ack_sender {
            let _ = sender.send(bytes);
        }
    }
}

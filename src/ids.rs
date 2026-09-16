use uuid::Uuid;

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn request_id() -> String {
    format!("req_{}", Uuid::new_v4().simple())
}

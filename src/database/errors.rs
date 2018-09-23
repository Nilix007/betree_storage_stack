#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    foreign_links {
        TreeError(::tree::Error);
        StoragePoolError(::vdev::Error);
        SerializationError(::bincode::Error);
        ConfigurationError(::storage_pool::configuration::Error);
    }
    errors {
        SplConfiguration
        InvalidSuperblock
        DoesNotExist
        AlreadyExists
        InUse
        InDestruction
    }
}

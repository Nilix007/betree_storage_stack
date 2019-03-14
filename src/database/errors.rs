#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    foreign_links {
        TreeError(crate::tree::Error);
        StoragePoolError(crate::vdev::Error);
        SerializationError(::bincode::Error);
        ConfigurationError(crate::storage_pool::configuration::Error);
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

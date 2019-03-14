#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        VdevError(crate::vdev::Error);
    }
    errors {
        DecompressionError
        DeserializationError
        SerializationError
        HandlerError
        CannotWriteBackError
        OutOfSpaceError
    }
}

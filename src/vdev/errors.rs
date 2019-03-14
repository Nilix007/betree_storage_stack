#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        Io(::std::io::Error);
        ChecksumError(crate::checksum::ChecksumError);
    }
    errors {
        ReadError(id: String)
        WriteError(id: String)
        SpawnError(error: String)
    }
}

impl From<!> for Error {
    fn from(_: !) -> Error {
        unreachable!()
    }
}

impl From<::futures::task::SpawnError> for Error {
    fn from(e: ::futures::task::SpawnError) -> Error {
        Error::from(ErrorKind::SpawnError(format!("{:?}", e)))
    }
}

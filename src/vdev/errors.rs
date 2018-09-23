#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        Io(::std::io::Error);
        ChecksumError(::checksum::ChecksumError);
    }
    errors {
        ReadError(id: String)
        WriteError(id: String)
    }
}

impl From<!> for Error {
    fn from(_: !) -> Error {
        unreachable!()
    }
}

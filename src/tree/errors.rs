#![allow(missing_docs, unused_doc_comments)]
error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        DmuError(crate::data_management::Error);
    }
    errors {
    }
}

#![allow(missing_docs, unused_doc_comment)]
error_chain! {
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        DmuError(::data_management::Error);
    }
    errors {
    }
}

use size::StaticSize;
use std::io;

/// No-op compression.
#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub struct None;

impl StaticSize for None {
    fn size() -> usize {
        0
    }
}

impl super::Compression for None {
    type Compress = Compress;

    fn decompress(&self, buffer: Box<[u8]>) -> io::Result<Box<[u8]>> {
        Ok(buffer)
    }

    fn compress(&self) -> Self::Compress {
        Compress::new()
    }
}

pub struct Compress(Vec<u8>);

impl Compress {
    fn new() -> Self {
        Compress(Vec::new())
    }
}

impl io::Write for Compress {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl super::Compress for Compress {
    fn finish(self) -> Box<[u8]> {
        self.0.into_boxed_slice()
    }
}

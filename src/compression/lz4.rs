use super::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum, Decoder, Encoder, EncoderBuilder};
use size::StaticSize;
use std::io::{self, Read};

/// LZ4 compression. (<https://github.com/lz4/lz4>)
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Lz4 {
    /// The compression level which describes the trade-off between
    /// compression ratio and compression speed.
    /// Lower level provides faster compression at a worse ratio.
    /// Maximum level is 16, higher values will count as 16.
    pub level: u8,
}

impl StaticSize for Lz4 {
    fn size() -> usize {
        1
    }
}

impl Compression for Lz4 {
    type Compress = Compress;

    fn decompress(&self, buffer: Box<[u8]>) -> io::Result<Box<[u8]>> {
        let mut output = vec![0; buffer.len()];
        Decoder::new(&buffer[..])?.read_to_end(&mut output)?;
        Ok(output.into_boxed_slice())
    }

    fn compress(&self) -> Self::Compress {
        let encoder = EncoderBuilder::new()
            .level(u32::from(self.level))
            .checksum(ContentChecksum::NoChecksum)
            .block_size(BlockSize::Max4MB)
            .block_mode(BlockMode::Linked)
            .build(Vec::new())
            .unwrap();
        Compress { encoder }
    }
}

pub struct Compress {
    encoder: Encoder<Vec<u8>>,
}

impl io::Write for Compress {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.encoder.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }
}

impl super::Compress for Compress {
    fn finish(self) -> Box<[u8]> {
        let (v, result) = self.encoder.finish();
        result.unwrap();
        v.into_boxed_slice()
    }
}

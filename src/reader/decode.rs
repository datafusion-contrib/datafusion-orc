pub mod boolean_rle;
pub mod float;
pub mod rle_v2;
mod util;
pub mod variable_length;

#[inline]
fn read_u8<R: std::io::Read>(reader: &mut R) -> Result<u8, std::io::Error> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

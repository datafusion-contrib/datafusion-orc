use crate::{error, proto, statistics::ColumnStatistics};

/// Stripe metadata parsed from the file tail metadata sections.
/// Does not contain the actual stripe bytes, as those are decoded
/// when they are required.
#[derive(Debug, Clone)]
pub struct StripeMetadata {
    /// Statistics of columns across this specific stripe
    column_statistics: Vec<ColumnStatistics>,
    /// Byte offset of start of stripe from start of file
    offset: u64,
    /// Byte length of index section
    index_length: u64,
    /// Byte length of data section
    data_length: u64,
    /// Byte length of footer section
    footer_length: u64,
    /// Number of rows in the stripe
    number_of_rows: u64,
}

impl StripeMetadata {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn index_length(&self) -> u64 {
        self.index_length
    }

    pub fn data_length(&self) -> u64 {
        self.data_length
    }

    pub fn footer_length(&self) -> u64 {
        self.footer_length
    }

    pub fn number_of_rows(&self) -> u64 {
        self.number_of_rows
    }

    pub fn column_statistics(&self) -> &[ColumnStatistics] {
        &self.column_statistics
    }

    pub fn footer_offset(&self) -> u64 {
        self.offset + self.index_length + self.data_length
    }
}

impl TryFrom<(&proto::StripeInformation, &proto::StripeStatistics)> for StripeMetadata {
    type Error = error::Error;

    fn try_from(
        value: (&proto::StripeInformation, &proto::StripeStatistics),
    ) -> Result<Self, Self::Error> {
        let column_statistics = value
            .1
            .col_stats
            .iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            column_statistics,
            offset: value.0.offset(),
            index_length: value.0.index_length(),
            data_length: value.0.data_length(),
            footer_length: value.0.footer_length(),
            number_of_rows: value.0.number_of_rows(),
        })
    }
}

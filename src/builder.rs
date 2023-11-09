use arrow::array::{ArrayBuilder, ArrayRef};

/// BoxedArrayBuilder
///
/// It implements the [ArrayBuilder] and [Sized] trait.
pub struct BoxedArrayBuilder {
    pub(crate) builder: Box<dyn ArrayBuilder>,
}

impl ArrayBuilder for BoxedArrayBuilder {
    fn len(&self) -> usize {
        self.builder.len()
    }

    fn finish(&mut self) -> ArrayRef {
        self.builder.finish()
    }

    fn finish_cloned(&self) -> ArrayRef {
        self.builder.finish_cloned()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.builder.as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.builder.as_any_mut()
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self.builder.into_box_any()
    }
}

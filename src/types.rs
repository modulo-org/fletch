use anyhow::{Result, anyhow};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum FletchValue {
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
}

pub trait FletchType: Sized + Send + Sync + 'static {
    fn data_type() -> DataType;
    fn new_builder(capacity: usize) -> ChannelBuilder;
    fn into_value(self) -> FletchValue;
}

macro_rules! fletch_type {
    ($ty:ty, $variant:ident, $builder:ident, $data_type:expr) => {
        impl FletchType for $ty {
            fn data_type() -> DataType {
                $data_type
            }

            fn new_builder(capacity: usize) -> ChannelBuilder {
                ChannelBuilder::$variant($builder::with_capacity(capacity))
            }

            fn into_value(self) -> FletchValue {
                FletchValue::$variant(self)
            }
        }
    };
}

fletch_type!(bool, Bool, BooleanBuilder, DataType::Boolean);
fletch_type!(u8, U8, UInt8Builder, DataType::UInt8);
fletch_type!(u16, U16, UInt16Builder, DataType::UInt16);
fletch_type!(u32, U32, UInt32Builder, DataType::UInt32);
fletch_type!(i32, I32, Int32Builder, DataType::Int32);
fletch_type!(i64, I64, Int64Builder, DataType::Int64);
fletch_type!(f32, F32, Float32Builder, DataType::Float32);
fletch_type!(f64, F64, Float64Builder, DataType::Float64);

impl FletchType for String {
    fn data_type() -> DataType {
        DataType::Utf8
    }

    fn new_builder(_capacity: usize) -> ChannelBuilder {
        ChannelBuilder::String(StringBuilder::new())
    }

    fn into_value(self) -> FletchValue {
        FletchValue::String(self)
    }
}

pub enum ChannelBuilder {
    Bool(BooleanBuilder),
    U8(UInt8Builder),
    U16(UInt16Builder),
    U32(UInt32Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    String(StringBuilder),
}

impl ChannelBuilder {
    pub fn append(&mut self, value: Option<FletchValue>) -> Result<()> {
        match (self, value) {
            (Self::Bool(builder), Some(FletchValue::Bool(value))) => builder.append_value(value),
            (Self::U8(builder), Some(FletchValue::U8(value))) => builder.append_value(value),
            (Self::U16(builder), Some(FletchValue::U16(value))) => builder.append_value(value),
            (Self::U32(builder), Some(FletchValue::U32(value))) => builder.append_value(value),
            (Self::I32(builder), Some(FletchValue::I32(value))) => builder.append_value(value),
            (Self::I64(builder), Some(FletchValue::I64(value))) => builder.append_value(value),
            (Self::F32(builder), Some(FletchValue::F32(value))) => builder.append_value(value),
            (Self::F64(builder), Some(FletchValue::F64(value))) => builder.append_value(value),
            (Self::String(builder), Some(FletchValue::String(value))) => {
                builder.append_value(value)
            }
            (builder, None) => builder.append_null(),
            _ => {
                return Err(anyhow!(
                    "channel value type does not match registered channel type"
                ));
            }
        }
        Ok(())
    }

    pub fn append_null(&mut self) {
        match self {
            Self::Bool(builder) => builder.append_null(),
            Self::U8(builder) => builder.append_null(),
            Self::U16(builder) => builder.append_null(),
            Self::U32(builder) => builder.append_null(),
            Self::I32(builder) => builder.append_null(),
            Self::I64(builder) => builder.append_null(),
            Self::F32(builder) => builder.append_null(),
            Self::F64(builder) => builder.append_null(),
            Self::String(builder) => builder.append_null(),
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::U8(builder) => Arc::new(builder.finish()),
            Self::U16(builder) => Arc::new(builder.finish()),
            Self::U32(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::F32(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
        }
    }
}

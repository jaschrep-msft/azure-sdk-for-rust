use std::str::FromStr;

#[derive(PartialEq, Debug)]
pub enum ContentRangeUnit {
    Bytes,
}
#[derive(PartialEq, Debug)]
pub struct ContentRange {
    pub unit: ContentRangeUnit,
    pub range_inclusive: Option<(u64, u64)>,
    pub size: Option<u64>,
}
impl FromStr for ContentRange {
    type Err = azure_core::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || {
            azure_core::Error::message(
                azure_core::error::ErrorKind::Other,
                format!("Failed to parse Content-Range header {}.", s),
            )
        };

        let st = s.trim();
        let (_unit, st) = st.split_at(st.find(char::is_whitespace).ok_or_else(err)?);
        let (range, size) = st.split_at(st.find('/').ok_or_else(err)?);

        let range = range.trim();
        let size = size[1..].trim();

        Ok(ContentRange {
            unit: ContentRangeUnit::Bytes,
            range_inclusive: if range == "*" {
                None
            } else {
                let (range_start, range_end) = range.split_at(range.find('-').ok_or_else(err)?);
                Some((
                    range_start.parse().map_err(|_| err())?,
                    range_end[1..].parse().map_err(|_| err())?,
                ))
            },
            size: if size == "*" {
                None
            } else {
                Some(size.parse().map_err(|_| err())?)
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::content_range::ContentRangeUnit;

    #[test]
    fn test_success() -> azure_core::Result<()> {
        let cases = [
            (
                "bytes 0-1023/1024",
                ContentRange {
                    unit: ContentRangeUnit::Bytes,
                    range_inclusive: Some((0, 1023)),
                    size: Some(1024),
                },
            ),
            (
                "bytes 30-40/70",
                ContentRange {
                    unit: ContentRangeUnit::Bytes,
                    range_inclusive: Some((30, 40)),
                    size: Some(70),
                },
            ),
            (
                "bytes */1024",
                ContentRange {
                    unit: ContentRangeUnit::Bytes,
                    range_inclusive: None,
                    size: Some(1024),
                },
            ),
            (
                "bytes 0-1023/*",
                ContentRange {
                    unit: ContentRangeUnit::Bytes,
                    range_inclusive: Some((0, 1023)),
                    size: None,
                },
            ),
        ];

        for (input, expected_output) in cases {
            let output: ContentRange = input.parse()?;
            assert_eq!(output, expected_output);
        }

        Ok(())
    }

    #[test]
    fn test_failure() -> azure_core::Result<()> {
        let cases = [
            "foo",
            "bytes0-1023/1024",
            "bytes 0 -1023/1024",
            "bytes 0- 1023/1024",
            //"bytes 0-1023 /1024",
            //"bytes 0-1023/ 1024",
            "bytes 0a-1023/1024",
            "bytes 0-1023a/1024",
            "bytes 0-1023/1024a",
            "bytes 10a00-1023/1024",
            "bytes 0-10a23/1024",
            "bytes 0-1023/10a24",
            "bytes 0-*/1024",
            "bytes *-1023/1024",
            "bytes 0-1023",
            "bytes 1024",
        ];

        for input in cases {
            let output: Result<ContentRange, _> = input.parse();
            assert!(
                output.is_err(),
                "Expected failure parsing content range '{}'. Was successful.",
                input
            );
        }

        Ok(())
    }
}

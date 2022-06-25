// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2022 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::fmt::{Debug, Display, Formatter, Result};

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum CurrencyType {
    Crypto,
    Fiat,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum OrderSide {
    Buy = 1,
    Sell = 2,
}

impl OrderSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }
}

impl From<&str> for OrderSide {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => panic!("Invalid `OrderSide` value, was {s}"),
        }
    }
}

// impl Into<&str> for OrderSide {
//     fn into(self) -> &'static str {
//         match self {
//             OrderSide::Buy => "BUY",
//             OrderSide::Sell => "SELL",
//         }
//     }
// }

// impl ToString for OrderSide {
//     fn to_string(&self) -> String {
//         match self {
//             OrderSide::Buy => "BUY",
//             OrderSide::Sell => "SELL",
//         }.parse().unwrap()
//     }
// }

impl Display for OrderSide {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.as_str())
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum AssetClass {
    FX = 1,
    Equity = 2,
    Commodity = 3,
    Metal = 4,
    Energy = 5,
    Bond = 6,
    Index = 7,
    Crypto = 8,
    Betting = 9,
}
impl From<&str> for AssetClass {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "FX" => AssetClass::FX,
            "EQUITY" => AssetClass::Equity,
            "COMMODITY" => AssetClass::Commodity,
            "METAL" => AssetClass::Metal,
            "ENERGY" => AssetClass::Energy,
            "BOND" => AssetClass::Bond,
            "INDEX" => AssetClass::Index,
            "CRYPTO" => AssetClass::Crypto,
            "BETTING" => AssetClass::Betting,
            _ => panic!("Invalid `AssetClass` value, was {s}"),
        }
    }
}
impl From<u8> for AssetClass {
    fn from(i: u8) -> Self {
        match i {
            1 => AssetClass::FX,
            2 => AssetClass::Equity,
            3 => AssetClass::Commodity,
            4 => AssetClass::Metal,
            5 => AssetClass::Energy,
            6 => AssetClass::Bond,
            7 => AssetClass::Index,
            8 => AssetClass::Crypto,
            9 => AssetClass::Betting,
            _ => panic!("Invalid `AssetClass` value, was {i}"),
        }
    }
}
impl AssetClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetClass::FX => "FX",
            AssetClass::Equity => "EQUITY",
            AssetClass::Commodity => "COMMODITY",
            AssetClass::Metal => "METAL",
            AssetClass::Energy => "ENERGY" ,
            AssetClass::Bond => "BOND" ,
            AssetClass::Index => "INDEX" ,
            AssetClass::Crypto => "CRYPTO",
            AssetClass::Betting => "BETTING",
        }
    }
}

impl Display for AssetClass {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.as_str())
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
pub enum AssetType {
    Spot = 1,
    Swap = 2,
    Future = 3,
    Forward = 4,
    CFD = 5,
    Option = 6,
    Warrant = 7,
}
impl From<&str> for AssetType {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "SPOT" => AssetType::Spot,
            "SWAP" => AssetType::Swap,
            "FUTURE" => AssetType::Future,
            "FORWARD" => AssetType::Forward,
            "CFD" => AssetType::CFD,
            "OPTION" => AssetType::Option,
            "WARRANT" => AssetType::Warrant,
            _ => panic!("Invalid `AssetType` value, was {s}"),
        }
    }
}
impl From<u8> for AssetType {
    fn from(i: u8) -> Self {
        match i {
            1 => AssetType::Spot,
            2 => AssetType::Swap,
            3 => AssetType::Future,
            4 => AssetType::Forward,
            5 => AssetType::CFD,
            6 => AssetType::Option,
            7 => AssetType::Warrant,
            _ => panic!("Invalid `AssetType` value, was {i}"),
        }
    }
}
impl AssetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetType::Spot => "SPOT",
            AssetType::Swap => "SWAP",
            AssetType::Future => "FUTURE",
            AssetType::Forward => "FORWARD",
            AssetType::CFD => "CFD",
            AssetType::Option => "OPTION",
            AssetType::Warrant => "WARRANT",
        }
    }
}

impl Display for AssetType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.as_str())
    }
}



#[repr(C)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum PriceType {
    Bid = 1,
    Ask = 2,
    Mid = 3,
    Last = 4,
}
impl PriceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PriceType::Bid => "BID",
            PriceType::Ask => "ASK",
            PriceType::Mid => "MID",
            PriceType::Last => "LAST",
        }
    }
}
impl Display for PriceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for PriceType {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "BID" => PriceType::Bid,
            "ASK" => PriceType::Ask,
            "MID" => PriceType::Mid,
            "LAST" => PriceType::Last,
            _ => panic!("Invalid `PriceType` value, was {s}"),
        }
    }
}
impl From<u8> for PriceType {
    fn from(i: u8) -> Self {
        match i {
            1 => PriceType::Bid,
            2 => PriceType::Ask,
            3 => PriceType::Mid,
            4 => PriceType::Last,
            _ => panic!("Invalid `Price` value, was {i}"),
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum BookLevel {
    L1_TBBO = 1,
    L2_MBP = 2,
    L3_MBO = 3,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum BookAction {
    Add = 1,
    Update = 2,
    Delete = 3,
    Clear = 4,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum DepthType {
    Volume = 1,
    Exposure = 2,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
#[allow(non_camel_case_types)]
pub enum BarAggregation {
    Tick = 1,
    TickImbalance = 2,
    TickRuns = 3,
    Volume = 4,
    VolumeImbalance = 5,
    VolumeRuns = 6,
    Value = 7,
    ValueImbalance = 8,
    ValueRuns = 9,
    Millisecond = 10,
    Second = 11,
    Minute = 12,
    Hour = 13,
    Day = 14,
    Week = 15,
    Month = 16,
}

impl From<&str> for BarAggregation {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "TICK" => BarAggregation::Tick,
            "TICK_IMBALANCE" => BarAggregation::TickImbalance,
            "TICK_RUNS" => BarAggregation::TickRuns,
            "VOLUME" => BarAggregation::Volume,
            "VOLUME_IMBALANCE" => BarAggregation::VolumeImbalance,
            "VOLUME_RUNS" => BarAggregation::VolumeRuns,
            "VALUE" => BarAggregation::Value,
            "VALUE_IMBALANCE" => BarAggregation::ValueImbalance,
            "VALUE_RUNS" => BarAggregation::ValueRuns,
            "MILLISECOND" => BarAggregation::Millisecond,
            "SECOND" => BarAggregation::Second,
            "MINUTE" => BarAggregation::Minute,
            "HOUR" => BarAggregation::Hour,
            "DAY" => BarAggregation::Day,
            "WEEK" => BarAggregation::Week,
            "MONTH" => BarAggregation::Month,
            _ => panic!("Invalid `BarAggregation` value, was {s}"),
        }
    }
}
impl From<u8> for BarAggregation {
    fn from(i: u8) -> Self {
        match i {
            1 => BarAggregation::Tick,
            2 => BarAggregation::TickImbalance,
            3 => BarAggregation::TickRuns,
            4 => BarAggregation::Volume,
            5 => BarAggregation::VolumeImbalance,
            6 => BarAggregation::VolumeRuns,
            7 => BarAggregation::Value,
            8 => BarAggregation::ValueImbalance,
            9 => BarAggregation::ValueRuns,
            10 => BarAggregation::Millisecond,
            11 => BarAggregation::Second,
            12 => BarAggregation::Minute,
            13 => BarAggregation::Hour,
            14 => BarAggregation::Day,
            15 => BarAggregation::Week,
            16 => BarAggregation::Month,
            _ => panic!("Invalid `BarAggregation` value, was {i}"),
        }
    }
}

impl BarAggregation {
    pub fn as_str(&self) -> &'static str {
        match self {
            BarAggregation::Tick => "TICK",
            BarAggregation::TickImbalance => "TICK_IMBALANCE",
            BarAggregation::TickRuns => "TICK_RUNS",
            BarAggregation::Volume => "VOLUME",
            BarAggregation::VolumeImbalance => "VOLUME_IMBALANCE",
            BarAggregation::VolumeRuns => "VOLUME_RUNS",
            BarAggregation::Value => "VALUE",
            BarAggregation::ValueImbalance => "VALUE_IMBALANCE",
            BarAggregation::ValueRuns => "VALUE_RUNS",
            BarAggregation::Millisecond => "MILLISECOND",
            BarAggregation::Second => "SECOND",
            BarAggregation::Minute => "MINUTE",
            BarAggregation::Hour => "HOUR",
            BarAggregation::Day => "DAY",
            BarAggregation::Week => "WEEK",
            BarAggregation::Month => "MONTH",
        }
    }
}

impl Display for BarAggregation {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.as_str())
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum AggregationSource {
    External = 1,
    Internal = 2,
}

impl AggregationSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            AggregationSource::External => "EXTERNAL",
            AggregationSource::Internal => "INTERNAL",
        }
    }
}

impl From<&str> for AggregationSource {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "EXTERNAL" => AggregationSource::External,
            "INTERNAL" => AggregationSource::Internal,
            _ => panic!("Invalid `AggregationSource` value, was {s}"),
        }
    }
}
impl From<u8> for AggregationSource {
    fn from(i: u8) -> Self {
        match i {
            1 => AggregationSource::External,
            2 => AggregationSource::Internal,
            _ => panic!("Invalid `AggregationSource` value, was {i}"),
        }
    }
}

impl Display for AggregationSource {
	fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.as_str())
    }
}
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OptionTag {
    None = 0,
    Some = 1
}
impl OptionTag {
    pub fn as_str(&self) -> &'static str {
        match self {
            OptionTag::None => "NONE",
            OptionTag::Some => "SOME",
        }
    }
}
impl Display for OptionTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.as_str())
    }
}

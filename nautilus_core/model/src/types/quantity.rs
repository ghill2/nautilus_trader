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

use crate::types::fixed::{f64_to_fixed_u64, fixed_u64_to_f64};
use nautilus_core::string::precision_from_str;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter, Result};
use std::hash::{Hash, Hasher};
use std::ops::{Add, AddAssign, Mul, MulAssign, Sub, SubAssign};

#[repr(C)]
#[derive(Eq, Clone, Default)]
pub struct Quantity {
    fixed: u64,
    pub precision: u8,
}

impl Quantity {
    pub fn new(value: f64, precision: u8) -> Self {
        assert!(value >= 0.0);

        Quantity {
            fixed: f64_to_fixed_u64(value, precision),
            precision,
        }
    }

    pub fn from_fixed(fixed: u64, precision: u8) -> Self {
        Quantity { fixed, precision }
    }

    pub fn new_from_str(input: &str) -> Self {
        let float_from_input = input.parse::<f64>();
        let float_res = match float_from_input {
            Ok(number) => number,
            Err(err) => panic!("Cannot parse `input` string '{}' as f64, {}", input, err),
        };
        Quantity::new(float_res, precision_from_str(input))
    }

    pub fn is_zero(&self) -> bool {
        self.fixed == 0
    }
    pub fn as_f64(&self) -> f64 {
        fixed_u64_to_f64(self.fixed)
    }
}

impl Hash for Quantity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fixed.hash(state)
    }
}

impl PartialEq for Quantity {
    fn eq(&self, other: &Self) -> bool {
        self.fixed == other.fixed
    }
}

impl PartialOrd for Quantity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.fixed.partial_cmp(&other.fixed)
    }

    fn lt(&self, other: &Self) -> bool {
        self.fixed.lt(&other.fixed)
    }

    fn le(&self, other: &Self) -> bool {
        self.fixed.le(&other.fixed)
    }

    fn gt(&self, other: &Self) -> bool {
        self.fixed.gt(&other.fixed)
    }

    fn ge(&self, other: &Self) -> bool {
        self.fixed.ge(&other.fixed)
    }
}

impl Ord for Quantity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.fixed.cmp(&other.fixed)
    }
}

impl Add for Quantity {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Quantity {
            fixed: self.fixed + rhs.fixed,
            precision: self.precision,
        }
    }
}

impl Sub for Quantity {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self::Output {
        Quantity {
            fixed: self.fixed - rhs.fixed,
            precision: self.precision,
        }
    }
}

impl Mul for Quantity {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self::Output {
        Quantity {
            fixed: self.fixed * rhs.fixed,
            precision: self.precision,
        }
    }
}

impl AddAssign for Quantity {
    fn add_assign(&mut self, other: Self) {
        self.fixed += other.fixed;
    }
}

impl AddAssign<u64> for Quantity {
    fn add_assign(&mut self, other: u64) {
        self.fixed += other;
    }
}

impl SubAssign for Quantity {
    fn sub_assign(&mut self, other: Self) {
        self.fixed -= other.fixed;
    }
}

impl SubAssign<u64> for Quantity {
    fn sub_assign(&mut self, other: u64) {
        self.fixed -= other;
    }
}

impl MulAssign<u64> for Quantity {
    fn mul_assign(&mut self, multiplier: u64) {
        self.fixed *= multiplier;
    }
}

impl Debug for Quantity {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{:.*}", self.precision as usize, self.as_f64())
    }
}

impl Display for Quantity {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{:.*}", self.precision as usize, self.as_f64())
    }
}

#[allow(unused_imports)] // warning: unused import: `std::fmt::Write as FmtWrite`
#[cfg(test)]
mod tests {
    use crate::types::quantity::Quantity;

    #[test]
    fn test_qty_new() {
        let qty = Quantity::new(0.00812, 8);

        assert_eq!(qty, qty);
        assert_eq!(qty.fixed, 8120000);
        assert_eq!(qty.precision, 8);
        assert_eq!(qty.as_f64(), 0.00812);
        assert_eq!(qty.to_string(), "0.00812000");
    }

    #[test]
    fn test_qty_minimum() {
        let qty = Quantity::new(0.000000001, 9);

        assert_eq!(qty.fixed, 1);
        assert_eq!(qty.to_string(), "0.000000001");
    }

    #[test]
    fn test_qty_is_zero() {
        let qty = Quantity::new(0.0, 8);

        assert_eq!(qty, qty);
        assert_eq!(qty.fixed, 0);
        assert_eq!(qty.precision, 8);
        assert_eq!(qty.as_f64(), 0.0);
        assert_eq!(qty.to_string(), "0.00000000");
        assert!(qty.is_zero());
    }

    #[test]
    fn test_qty_precision() {
        let qty = Quantity::new(1.001, 2);

        assert_eq!(qty.fixed, 1000000000);
        assert_eq!(qty.to_string(), "1.00");
    }

    #[test]
    fn test_qty_new_from_str() {
        let qty = Quantity::new_from_str("0.00812000");

        assert_eq!(qty, qty);
        assert_eq!(qty.fixed, 8120000);
        assert_eq!(qty.precision, 8);
        assert_eq!(qty.as_f64(), 0.00812);
        assert_eq!(qty.to_string(), "0.00812000");
    }

    #[test]
    fn test_qty_equality() {
        assert_eq!(Quantity::new(1.0, 1), Quantity::new(1.0, 1));
        assert_eq!(Quantity::new(1.0, 1), Quantity::new(1.0, 2));
        assert_ne!(Quantity::new(1.1, 1), Quantity::new(1.0, 1));
        assert!(!(Quantity::new(1.0, 1) > Quantity::new(1.0, 2)));
        assert!(Quantity::new(1.1, 1) > Quantity::new(1.0, 1));
        assert!(Quantity::new(1.0, 1) >= Quantity::new(1.0, 1));
        assert!(Quantity::new(1.0, 1) >= Quantity::new(1.0, 2));
        assert!(!(Quantity::new(1.0, 1) < Quantity::new(1.0, 2)));
        assert!(Quantity::new(0.9, 1) < Quantity::new(1.0, 1));
        assert!(Quantity::new(0.9, 1) <= Quantity::new(1.0, 2));
        assert!(Quantity::new(0.9, 1) <= Quantity::new(1.0, 1));
    }

    #[test]
    fn test_qty_display() {
        use std::fmt::Write as FmtWrite;
        let input_string = "44.12";
        let qty = Quantity::new_from_str(&input_string);
        let mut res = String::new();

        write!(&mut res, "{}", qty).unwrap();
        assert_eq!(res, input_string);
        assert_eq!(qty.to_string(), input_string);
    }
}
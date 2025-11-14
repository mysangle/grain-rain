
use rustyline::{Completer, Helper, Hinter, Validator, highlight::Highlighter};

#[derive(Helper, Completer, Hinter, Validator)]
pub struct GrainHelper {

}

impl Highlighter for GrainHelper {
    
}


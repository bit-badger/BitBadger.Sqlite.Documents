namespace BitBadger.Sqlite.Documents

/// The types of logical operations available for JSON fields
[<Struct>]
type Op =
    /// Equals (=)
    | EQ = 0
    /// Greater Than (>)
    | GT = 1
    /// Greater Than or Equal To (>=)
    | GE = 2
    /// Less Than (<)
    | LT = 4
    /// Less Than or Equal To (<=)
    | LE = 5

/// Convert a C# Op to an F# Op
module internal Op =
    
    module FS = BitBadger.Sqlite.FSharp.Documents
    
    /// Convert the C# Op to an F# Op
    let convert (op: Op) =
        match op with
        | Op.EQ -> FS.Op.EQ
        | Op.GT -> FS.Op.GT
        | Op.GE -> FS.Op.GE
        | Op.LT -> FS.Op.LT
        | Op.LE -> FS.Op.LE
        | it -> invalidArg (nameof op) $"The operation {it} is not defined"

open Base

type e = {
    mutable sum : Int32.t;
    mutable count : Int32.t;
    mutable min : Int32.t;
    mutable max : Int32.t;
}

type t

val empty : unit -> t

val record_reading : t -> string -> Int32.t -> t

val to_list : t -> (string * e) list

val merge : t list -> t

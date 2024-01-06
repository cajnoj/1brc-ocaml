open Base

type e = {
    mutable sum : Int32.t;
    mutable count : Int32.t;
    mutable min : Int32.t;
    mutable max : Int32.t;
}

type t = (string, e) Hashtbl.t

let empty () = Hashtbl.create ~size:450 (module String)

let to_list t = 
    Hashtbl.fold ~f:(fun ~key ~data acc -> (key, data) :: acc) ~init:[] t

let record_reading t key reading =
    let data = match Hashtbl.find t key with
        | None -> { sum=reading; count=1l; min=reading; max=reading}
        | Some x -> (
            x.sum <- (Int32.(+) x.sum reading);
            x.count <- (Int32.(+) x.count 1l);
            if (Int32.compare reading x.min) = -1
            then
                x.min <- reading
            else 
                if (Int32.compare reading x.max) = 1
                then
                    x.max <- reading;
            x
        )
    in
        Hashtbl.set t ~key ~data;
        t

let merge l =
    let res = empty () in
    List.iter l ~f:(fun t ->
        Hashtbl.iteri t ~f:(fun ~key ~data ->
            let resdata = match Hashtbl.find res key with
                | None -> data
                | Some x -> (
                    x.sum <- Int32.(+) x.sum data.sum;
                    x.count <- Int32.(+) x.count data.count;
                    if (Int32.compare data.min x.min) = (-1)
                    then
                        x.min <- data.min;
                    if (Int32.compare data.max x.max) = 1
                    then
                        x.max <- data.max;
                    x
                )
            in
            Hashtbl.set res ~key ~data:resdata
        )
    );
    res


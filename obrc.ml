open Stdio
open Base

type state = In_city | In_temp

let formatted_time () =
    let now = Unix.gettimeofday () in
    let millis = (now -. Stdlib.Float.floor now) *. 1000.0 in
    let local_time = Unix.localtime now in
    Printf.sprintf "%02d:%02d:%02d.%03d"
    local_time.Unix.tm_hour
    local_time.Unix.tm_min
    local_time.Unix.tm_sec
    (Int.of_float millis)

let eprintf msg =
    Stdlib.Printf.eprintf "[%s] %s\n" (formatted_time ()) msg;
    Stdlib.flush Out_channel.stderr

let traverse readbuf startpos endpos =
    eprintf (Printf.sprintf "traversing buffer %d .. %d" startpos endpos);
    let c = ref ' ' in
    let pos = ref startpos in
    (* Seek in reverse a start of line *)
    if !pos > 0 then (
        while not (Char.equal !c '\n') do
            c := Buffer.nth readbuf !pos;
            pos := !pos - 1
        done;
        pos := !pos + 2;
    );
    (* Traverse forward *)
    let a = ref (Aggregator.empty ()) in
    let state = ref In_city in
    let city = ref "" in
    let tempbuf = Buffer.create 30 in
    while !pos < endpos do
        c := Buffer.nth readbuf !pos;
        (match !state with
        | In_city ->
            if not (Char.equal !c ';')
            then Buffer.add_char tempbuf !c
            else (
                state := In_temp;
                city := Buffer.contents tempbuf;
                Buffer.clear tempbuf
            )
        | In_temp -> 
                match !c with
        | '.' -> ()
        | '\n' ->
            a := Aggregator.record_reading
                  !a !city (Int32.of_string (Buffer.contents tempbuf));
           Buffer.clear tempbuf;
           state := In_city
        | _ -> Buffer.add_char tempbuf !c);
        pos := !pos + 1
    done;
    eprintf (Printf.sprintf "finished traversing %d .. %d" startpos endpos);
    !a

let read filename =
    eprintf "Handling file";
    let ic = In_channel.create filename in
    let stats = Unix.stat filename in
    let file_size = stats.Unix.st_size in
    eprintf "creating buffer";
    let readbuf = Buffer.create file_size in
    eprintf "reading file";
    match In_channel.input_buffer ic readbuf ~len:file_size with
    | None -> failwith "Read size mismatch";
    | Some () -> (());
    let argv = Sys.get_argv () in
    let parallelism = (Int.of_string argv.(1)) in
    let rec spawn n =
        let startpos = (n - 1) * file_size / parallelism in
        let endpos = n * file_size / parallelism in
        if n = 1 
        then [Stdlib.Domain.spawn (fun _ ->
                    traverse readbuf startpos endpos )]
        else (Stdlib.Domain.spawn (fun _ ->
                    traverse readbuf startpos endpos
                )) :: spawn (n - 1)
    in
    let domains = spawn parallelism
    in
    let rec join dl rl = 
        match dl with
        | [] -> rl
        | h::t -> join t ((Stdlib.Domain.join h) :: rl)
    in
    Aggregator.merge (join domains [])

let main () =
    let eprintf_in_pipe msg a =
        eprintf msg;
        a
    in
    Stdlib.Printf.eprintf "starting";
    Stdlib.flush_all ();
    let is_first = ref true in
    printf "{";
    read "measurements.txt"
    |> eprintf_in_pipe "converting to list"
    |> Aggregator.to_list
    |> eprintf_in_pipe "sorting"
    |> List.sort ~compare:(fun (x, _) (y, _) -> String.compare x y)
    |> eprintf_in_pipe "printing"
    |> List.iter 
        ~f:(fun (city, r) -> 
            let open Aggregator in
            let first = if !is_first then "" else ", " in
            is_first := false;
                printf
                    "%s%s=%.1f/%.1f/%.1f"
                    first
                    city
                    ((Int32.to_float r.min) /. 10.0)
                    ((Int32.to_float r.sum) /. (Int32.to_float r.count) /. 10.0)
                    ((Int32.to_float r.max) /. 10.0)

                    );
    printf "}\n"

let () =
    main ()

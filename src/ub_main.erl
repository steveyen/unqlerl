-module(ub_main).

-record(ub_query, {ref, % From make_ref().
                   text,
                   matchers,
                   where_fun}).

-compile(export_all).

test() ->
    2 = 2,
    ok.

% -------------------------------------------------

% Example of 3 table nested loop join, showing how we derive
% the generic execute_nlj() function.
%
% The scan() implementation might buffer T1 & T2 rows for replay
% against T3, reversing the normal or implicit table access order
% of the nested-loop join.
%
% The scan() implementation might also perform a hash-join or a
% merge-sort-join, such as by first hashing T2 by the scan_prep() and
% using the accumulator state to pass around the hash table or merge
% cursors.
%
% The accumulator state might need reduce'ing across distributed nodes.
%
% The final execute_nlj() avoids tons of closure creation, by
% pre-creating all its closures before any scan() calls.
% This allows scan()/scan_prep() to re-order any visits
% without extra cost.
%
execute_nlj_3_example_a(ClientCB, Query, [T0, T1, T2]) ->
    % Here's how a nested loop would naively look...
    %
    {ok, ScanPrep0, InitialAcc0} = scan_prep(Query, 0, T0, [], outer),
    scan(T0, ScanPrep0, InitialAcc0, [],
        fun(Join0, Acc0) ->
            {ok, ScanPrep1, InitialAcc1} = scan_prep(Query, 1, T1, Join0, Acc0),
            scan(T1, ScanPrep1, InitialAcc1, Join0,
                fun(Join1, Acc1) ->
                    {ok, ScanPrep2, InitialAcc2} = scan_prep(Query, 2, T2, Join1, Acc1),
                    scan(T2, ScanPrep2, InitialAcc2, Join1,
                        fun(Join2, Acc2) ->
                            execute_where(ClientCB, Query, Join2, Acc2)
                        end)
                end)
        end).

execute_nlj_3_example_b(ClientCB, Query, [T0, T1, T2] = _Tables) ->
    % Here we move the closures outside the scan calls.
    %
    Fun2 = fun(Join2, Acc2) ->
               execute_where(ClientCB, Query, Join2, Acc2)
           end,
    Fun1 = fun(Join1, Acc1) ->
               {ok, ScanPrep2, InitialAcc2} = scan_prep(Query, 2, T2, Join1, Acc1),
               scan(T2, ScanPrep2, InitialAcc2, Join1, Fun2)
           end,
    Fun0 = fun(Join0, Acc0) ->
               {ok, ScanPrep1, InitialAcc1} = scan_prep(Query, 1, T1, Join0, Acc0),
               scan(T1, ScanPrep1, InitialAcc1, Join0, Fun1)
           end,
    {ok, ScanPrep0, InitialAcc0} = scan_prep(Query, 0, T0, [], outer),
    scan(T0, ScanPrep0, InitialAcc0, [], Fun0).

execute_nlj(_ClientCB, _Query, []) ->
    {ok, no_tables};
execute_nlj(ClientCB, Query, Tables) ->
    % A generic nested-loop-join implementation for joining N number
    % of tables, and which creates only N + 1 visitor functions/closures.
    %
    NTables = lists:length(Tables),
    InnerVisitorFun = fun(Join, Acc) ->
                          execute_where(ClientCB, Query, Join, Acc)
                      end,
    [VisitorFunOuter | _] =
        lists:foldr(fun(Table, [LastVisitorFun | _ ] = VisitorFuns) ->
                        [fun(Join, Acc) ->
                             QueryPart = NTables - lists:length(VisitorFuns),
                             {ok, ScanPrep, AccNext} =
                                  scan_prep(Query, QueryPart, Table, Join, Acc),
                             scan(Table, ScanPrep, AccNext, Join, LastVisitorFun)
                         end | VisitorFuns]
                    end,
                    Tables,
                    [InnerVisitorFun]),
    VisitorFunOuter([], outer).

scan([], _ScanPrep, Acc, _JoinPrev, _DocVisitorFun) ->
    {ok, Acc};
scan([Doc | Rest], ScanPrep, Acc, JoinPrev, DocVisitorFun) ->
    {ok, AccNext} = DocVisitorFun([Doc | JoinPrev], Acc),
    scan(Rest, ScanPrep, AccNext, JoinPrev, DocVisitorFun).

scan_prep(_Query, _QueryPart, _Table, _Join, Acc) ->
    {ok, fun(_) -> true end, Acc}.

execute_where(ClientCB, Query, Join, Acc) ->
    case where_satisfied(Query, Join) of
        true -> ClientCB(result, Query, Join, Acc);
        false -> ok
    end,
    Acc.

where_satisfied(#ub_query{where_fun=undefined}, _Join) ->
    true;
where_satisfied(#ub_query{where_fun=WhereFun}, Join) ->
    WhereFun(Join).

prepare_hash(Query, QueryPart, Table) ->
    Ref = erlang:make_ref(),
    erlang:put(Ref, dict:new()),
    Path = query_path(Query, QueryPart, Table),
    {ok, ScanPrep, InitialAcc} = scan_prep(Query, QueryPart, hash_Table, populate, populate),
    scan(Table, ScanPrep, InitialAcc, [],
         fun([Doc], _Acc) ->
           V = doc_path_deref(Doc, Path),
           erlang:put(Ref, dict:store(Path, V, erlang:get(Ref)))
         end),
    Result = erlang:erase(Ref),
    Result.

query_path(_Query, _QueryPart, _Table) ->
    undefined.

doc_path_deref(_Doc, _Path) ->
    undefined.

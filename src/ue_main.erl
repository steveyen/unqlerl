-module(ue_main).

-record(ue_query, {ref, % From make_ref().
                   text,
                   matchers,
                   where_fun}).

-compile(export_all).

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
    % Compared to execute_nlj_3_example_a(), we've move the closures
    % outside the scan calls, so there's a lot less closure creation.
    % The execute_nlj() implementation is a generification of this
    % function.
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
    NTables = length(Tables),
    InnerVisitorFun = fun(Join, Acc) ->
                          execute_where(ClientCB, Query, Join, Acc)
                      end,
    [VisitorFunOuter | _] =
        lists:foldr(fun(Table, [LastVisitorFun | _ ] = VisitorFuns) ->
                        [fun(Join, Acc) ->
                             QueryPart = NTables - length(VisitorFuns),
                             {ok, ScanPrep, AccNext} =
                                  scan_prep(Query, QueryPart, Table, Join, Acc),
                             scan(Table, ScanPrep, AccNext, Join, LastVisitorFun)
                         end | VisitorFuns]
                    end,
                    [InnerVisitorFun],
                    Tables),
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
    {ok, Acc}.

where_satisfied(#ue_query{where_fun=undefined}, _Join) ->
    true;
where_satisfied(#ue_query{where_fun=WhereFun}, Join) ->
    WhereFun(Join).

% -------------------------------------------------

test() ->
    ok = scan_test(),
    ok = nlj_test(),
    ok.

scan_test() ->
    {ok, acc} = scan([], unused, acc, unused, unused),
    {ok, []} =
         scan([], ok, [], [],
              fun([Doc | _], Acc) -> {ok, [Doc | Acc]} end),
    {ok, [1]} =
         scan([1], ok, [], [],
              fun([Doc | _], Acc) -> {ok, [Doc | Acc]} end),
    {ok, [3,2,1]} =
         scan([1,2,3], ok, [], [],
              fun([Doc | _], Acc) -> {ok, [Doc | Acc]} end),
    ok.

nlj_test() ->
    {ok, no_tables} =
        execute_nlj(unused, unused, []),
    {ok, outer} =
        execute_nlj(fun(Kind, _Query, _Join, _Acc) ->
                        Kind = unexpected_invocation
                    end,
                    #ue_query{},
                    [[]]),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        Join = [1],
                        Acc = outer
                    end,
                    #ue_query{},
                    [[1]]),
    put(joins, []),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        put(joins, [Join | get(joins)]),
                        Acc = outer
                    end,
                    #ue_query{},
                    [[1, 2, 3]]),
    [[3],[2],[1]] = get(joins),
    put(joins, []),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        put(joins, [Join | get(joins)]),
                        Acc = outer
                    end,
                    #ue_query{},
                    [[1, 2, 3], [a, b]]),
    [[b,3],[a,3],[b,2],[a,2],[b,1],[a,1]] = get(joins),
    put(joins, []),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        put(joins, [Join | get(joins)]),
                        Acc = outer
                    end,
                    #ue_query{},
                    [[1, 2, 3], []]),
    [] = get(joins),
    put(joins, []),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        put(joins, [Join | get(joins)]),
                        Acc = outer
                    end,
                    #ue_query{},
                    [[], [1, 2, 3]]),
    [] = get(joins),
    put(joins, []),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        put(joins, [Join | get(joins)]),
                        Acc = outer
                    end,
                    #ue_query{},
                    [[1, 2, 3], [a, b], []]),
    [] = get(joins),
    put(joins, []),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        put(joins, [Join | get(joins)]),
                        Acc = outer
                    end,
                    #ue_query{},
                    [[1], [a], [x]]),
    [[x,a,1]] = get(joins),
    put(joins, []),
    {ok, outer} =
        execute_nlj(fun(Kind, Query, Join, Acc) ->
                        Kind = result,
                        Query = #ue_query{},
                        put(joins, [Join | get(joins)]),
                        Acc = outer
                    end,
                    #ue_query{},
                    [[1, 2, 3], [a, b], [x, y]]),
    [[y,b,3],[x,b,3],
     [y,a,3],[x,a,3],
     [y,b,2],[x,b,2],
     [y,a,2],[x,a,2],
     [y,b,1],[x,b,1],
     [y,a,1],[x,a,1]] = get(joins),
    ok.

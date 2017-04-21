% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_mrview_util_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").



couch_mrview_util_test_() ->
    [
         ?_assertEqual(0, validate_group_level(undefined, undefined)),
         ?_assertEqual(exact, validate_group_level(true, undefined)),
         ?_assertEqual(0, validate_group_level(false, undefined)),
         ?_assertEqual(1, validate_group_level(undefined, 1)),
         ?_assertEqual(0, validate_group_level(true, 0)),
         ?_assertEqual(0, validate_group_level(undefined, 0)),
         ?_assertEqual(1, validate_group_level(true, 1)),
         ?_assertEqual(0, validate_group_level(false, 0)),
         ?_assertThrow({query_parse_error,
              <<"Can't specify group=false and group_level>0 at the same time">>},
              validate_group_level(false,1))
    ].

convert_mrargs_upgrade_test_() ->
    LatestVsn = couch_mrview_util:record_vsn(#mrargs{}),
    Versions = lists:seq(-1, LatestVsn - 1),
    Cases = [{LatestVsn, LatestVsn} | [{V, V + 1} || V <- Versions]],
    {
        "Upgrade #mrargs{} tests",
        [ convert_mrargs_test(From, To) || {From, To} <- Cases ]
    }.

convert_mrargs_downgrade_test_() ->
    LatestVsn = couch_mrview_util:record_vsn(#mrargs{}),
    Versions = lists:seq(LatestVsn, 0, -1),
    Cases = [{LatestVsn, LatestVsn} | [{V, V - 1} || V <- Versions]],
    {
        "Downgrade #mrargs{} tests",
        [ convert_mrargs_test(From, To) || {From, To} <- Cases ]
    }.

convert_mrargs_test_() ->
    Current = #mrargs{},
    Versions = lists:seq(-1, couch_mrview_util:record_vsn(Current)),
    Cases = couch_tests_combinatorics:product([Versions, Versions]),
    {
        "Combinatorial #mrargs{} convert record tests",
        [ convert_mrargs_test(From, To) || [From, To] <- Cases ]
    }.


convert_mrargs_test(From, To) ->
    TestId = lists:flatten(io_lib:format("~w -> ~w", [From, To])),
    {TestId, ?_assertEqual(
        mrargs(To), couch_mrview_util:convert_record(From, To, mrargs(From))
    )}.

validate_group_level(Group, GroupLevel) ->
    Args0 = #mrargs{group=Group, group_level=GroupLevel, view_type=red},
    Args1 = couch_mrview_util:validate_args(Args0),
    Args1#mrargs.group_level.


mrargs(-1) ->
    {
        mrargs,
        view_type,
        reduce,

        preflight_fun,

        start_key,
        start_key_docid,
        end_key,
        end_key_docid,
        keys,

        direction,
        limit,
        skip,
        group_level,
        group,
        false, %% stale
        multi_get,
        inclusive_end,
        include_docs,
        doc_options,
        update_seq,
        conflicts,
        callback,
        sorted,
        extra
    };
mrargs(0) ->
    {
        mrargs,
        view_type,
        reduce,

        preflight_fun,

        start_key,
        start_key_docid,
        end_key,
        end_key_docid,
        keys,

        direction,
        limit,
        skip,
        group_level,
        group,
        false, %% stable
        true, %% update,
        multi_get,
        inclusive_end,
        include_docs,
        doc_options,
        update_seq,
        conflicts,
        callback,
        sorted,
        extra
    };
mrargs(1) ->
    {
        mrargs,
        1,
        view_type,
        reduce,

        preflight_fun,

        start_key,
        start_key_docid,
        end_key,
        end_key_docid,
        keys,

        direction,
        limit,
        skip,
        group_level,
        group,
        false, %% stable
        true, %% update,
        multi_get,
        inclusive_end,
        include_docs,
        doc_options,
        update_seq,
        conflicts,
        callback,
        sorted,
        extra
    }.

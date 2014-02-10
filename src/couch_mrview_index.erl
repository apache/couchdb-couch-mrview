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

-module(couch_mrview_index).


-export([get/2]).
-export([init/2, open/2, close/1, reset/1, delete/1]).
-export([start_update/3, purge/4, process_doc/3, finish_update/1, commit/1]).
-export([compact/3, swap_compacted/2]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


get(Property, State) ->
    case Property of
        db_name ->
            State#mrst.db_name;
        idx_name ->
            State#mrst.idx_name;
        signature ->
            State#mrst.sig;
        update_seq ->
            State#mrst.update_seq;
        purge_seq ->
            State#mrst.purge_seq;
        update_options ->
            Opts = State#mrst.design_opts,
            IncDesign = couch_util:get_value(<<"include_design">>, Opts, false),
            LocalSeq = couch_util:get_value(<<"local_seq">>, Opts, false),
            if IncDesign -> [include_design]; true -> [] end
                ++ if LocalSeq -> [local_seq]; true -> [] end;
        info ->
            #mrst{
                fd = Fd,
                sig = Sig,
                language = Lang,
                update_seq = UpdateSeq,
                purge_seq = PurgeSeq
            } = State,
            {ok, FileSize} = couch_file:bytes(Fd),
            {ok, ActiveSize} = couch_mrview_util:active_size(State),
            {ok, ExternalSize} = couch_mrview_util:external_size(State),
            {ok, [
                {signature, list_to_binary(couch_index_util:hexsig(Sig))},
                {language, Lang},
                {disk_size, FileSize},
                {data_size, ActiveSize},
                {sizes, {[
                    {file, FileSize},
                    {active, ActiveSize},
                    {external, ExternalSize}
                ]}},
                {update_seq, UpdateSeq},
                {purge_seq, PurgeSeq}
            ]};
        Other ->
            throw({unknown_index_property, Other})
    end.


init(Db, DDoc) ->
    couch_mrview_util:ddoc_to_mrst(couch_db:name(Db), DDoc).


open(Db, State) ->
    #mrst{
        db_name=DbName,
        sig=Sig
    } = State,
    IndexFName = couch_mrview_util:index_file(DbName, Sig),
    case couch_mrview_util:open_file(IndexFName) of
        {ok, Fd} ->
            case (catch couch_file:read_header(Fd)) of
                {ok, {Sig, Header}} ->
                    % Matching view signatures.
                    NewSt = couch_mrview_util:init_state(Db, Fd, State, Header),
                    {ok, NewSt#mrst{fd_monitor=erlang:monitor(process, Fd)}};
                _ ->
                    NewSt = couch_mrview_util:reset_index(Db, Fd, State),
                    {ok, NewSt#mrst{fd_monitor=erlang:monitor(process, Fd)}}
            end;
        {error, Reason} = Error ->
            ?LOG_ERROR("Failed to open view file '~s': ~s",
                       [IndexFName, file:format_error(Reason)]),
            Error
    end.


close(State) ->
    erlang:demonitor(State#mrst.fd_monitor, [flush]),
    couch_file:close(State#mrst.fd).


delete(#mrst{db_name=DbName, sig=Sig}=State) ->
    couch_file:close(State#mrst.fd),
    catch couch_mrview_util:delete_files(DbName, Sig).


reset(State) ->
    couch_util:with_db(State#mrst.db_name, fun(Db) ->
        NewState = couch_mrview_util:reset_index(Db, State#mrst.fd, State),
        {ok, NewState}
    end).


start_update(PartialDest, State, NumChanges) ->
    couch_mrview_updater:start_update(PartialDest, State, NumChanges).


purge(Db, PurgeSeq, PurgedIdRevs, State) ->
    couch_mrview_updater:purge(Db, PurgeSeq, PurgedIdRevs, State).


process_doc(Doc, Seq, State) ->
    couch_mrview_updater:process_doc(Doc, Seq, State).


finish_update(State) ->
    couch_mrview_updater:finish_update(State).


commit(State) ->
    Header = {State#mrst.sig, couch_mrview_util:make_header(State)},
    couch_file:write_header(State#mrst.fd, Header).


compact(Db, State, Opts) ->
    couch_mrview_compactor:compact(Db, State, Opts).


swap_compacted(OldState, NewState) ->
    couch_mrview_compactor:swap_compacted(OldState, NewState).


luact
=====

framework for writing robust, scalable network application, heavily inspired by celluloid(ruby) and Orleans(.NET).




概要
===

luactは複数のスレッドやマシン上で強調させてプログラムを動作させる、並行アプリケーションを容易に作成するためのフレームワークです。以下の特徴を備えています。

1. プログラミングの容易さ 

 luactは[celluloid](https://github.com/celluloid/celluloid)と類似した、並行プログラミングモデルである[actorモデル](http://www.wikiwand.com/ja/%E3%82%A2%E3%82%AF%E3%82%BF%E3%83%BC%E3%83%A2%E3%83%87%E3%83%AB)を採用しており、actor同士のメッセージングをactorオブジェクトに対するメソッド呼び出しとして行います。これにより、通常のプログラミングに慣れ親しんだ人が、自然に複数のオブジェクトを連携させるのと同じ感覚でプログラミングを行うだけで、このモデルが提供する以下のような利点を享受することができます。
 - 動作を予測しやすい並行プログラミング：通常の並行プログラミングは、任意のタイミングで割り込みが可能なスレッドを扱う場合、どこで予想外に別の処理が割り込んでくるかが想像しにくく、また問題が発生した場合に再現もさせにくいという難しさがあります。luactではすべての並行処理は協調的マルチタスキングによって実行されるため、並列で実行される他の処理が現在の処理に割り込んでくるタイミングが単純なルールにより決定しており(セクション：「並列実行と同期」参照）、あるコードの処理順を予測することが通常のマルチスレッドプログラムよりも圧倒的に簡単です。mutexやsemaphoreといったOSスレッドレベルの同期プリミティブなどを管理する必要もありません。
 - リモート、ローカル、別スレッドを意識しない自動的な通信処理：メッセージングは、オブジェクトの場所に寄らず、オブジェクトの位置を表すuuidと、呼び出すメッセージの名前、パラメーターだけによって、すべて同じ形で行うことができます。したがってプログラマーは何かサーバー内の処理を記述する際、関連するリソースがどこに存在し、どのようにアクセスしなければならないか、といったローレベルの要素に気をとられることなく、オブジェクト同士の相互作用を記述することに集中することができます。また、protocol bufferのように、メッセージを足す時にメッセージ定義ファイルを変更してコンパイルする、といった煩雑な作業を行う必要はありません。すべてのメッセージングの送受信処理は動的に生成されます。
 - マイクロサービス化：アクターベースでプログラムを構成することの利点は、プログラマーがプログラムを幾つかのアクターに分割しようと考えることによって、自然にプログラムがいくつかのマイクロサービスのされる形になることです。そのようにして分割されたアクターは相互に処理が独立しているためメンテナンスが容易であり、上記のように通信処理が完全に隠蔽されているため、テストも関数の呼び出しとその戻り値にたいして行えばいいため簡単です。

 celluloidベースのプログラミングモデルが提供する利点に加えて、luactは以下のようなプラスアルファを提供しています。
 - ユーザーが記述するスクリプトのシンタクスは[lua5.1](lua.org/manual/5.1/)であり、初学者にも理解、習得することが容易です。
 - 多数の計算ノードを連携させる必要があるような、分散コンピューティング環境上で、Orleansからinspireされた、distribute celluloid(dcell)よりも優れたセマンティクスを提供しています(後ほど出てくる仮想actorをご覧ください）
 - luactは[celluloid](https://github.com/celluloid/celluloid)より柔軟であり、多様なluaのオブジェクトをcelluloidよりも単純にactor化することができます。また、actorに対するメソッド呼び出しにprefixという仕組みを導入しており、１つのメッセージを様々なやり方で呼び出すことができます。これにより、コードの分かりやすさと、柔軟な並行処理の記述を両立しています。


2. 耐障害性
 
 celluloidと同様に、actorをsuperviseすることができ、シングルノードでのfault torelanceを提供しています。それだけでなく、Microsoft Researchが開発した[Orleans](https://github.com/dotnet/orleans)のような、分散環境においてノードの故障が発生する場合でも可用性を可能な限り維持する仕組みも備えています(実装中）。


3. 速度
 
 luactはluaJITに最適化されて実装されています。具体的には、通常この手のサーバーアプリケーションに見られるような、下層をC/C++で書いて、データをスクリプト言語のVMに受け渡すような構成ではなく、最も下層のnetwork IOのレイヤーから[luaJIT FFI](http://luajit.org/ext_ffi_tutorial.html)を多用して書かれています。また、luaJIT FFIでシステムで利用するデータ構造を定義することによって、可能な限り自前でメモリを管理しています。

 これにより、a)gc負荷の削減、b)tracing JITによる最適化の効果を最大化、c)Cレイヤーからスクリプトレイヤーにデータを受け渡しする際のオーバーヘッドの最小化が実現されています。

　
4. 運用

 サーバープログラムは書いただけではダメで長期間にわたって障害から復旧し、内容を更新しつつ動作させ続けなくてはなりません。この点luactは上で述べた耐障害性の高さだけではなく、[yue](http://github.com/umegaya/yue)を使ってdockerと強力に統合されることによって、運用のしやすさも提供します。
 

TODO : 他とのbenchmark comparison (especially Orleans)




アーキテクチャー
========

TODO : 複数スレッドとその中で走るたくさんのtentacle, あとraft/gossipが動いている絵




使い始める
========

luactのフロントエンドである[yue](http://github.com/umegaya/yue)を利用することで容易に使い始めることができます。
``` bash
git clone http://github.com/umegaya/yue.git
## 4スレッドでhello.luaを実行（初回はdocker imageのダウンロード、C headerのコンパイルのため時間がかかります）
cd yue && ./yue -c 4 sample/hello.lua
```




actor
=======

luactの基本的な実行単位であるactorについて説明します。

### actorの作成
``` lua
local a = luact({
	id = 100,
	hoge = function (self)
		return self.id
	end
})
```

luactはグローバルに定義されているシンボル名です。これにluaのtableを与えた結果帰ってきた*a*がactorです。actorになりうるluaのデータ型はtable, cdataで、これらのデータがキーに対する値として保持しているデータのうち、呼び出し可能なものがactorに対して送信可能なメッセージとして扱われます。上記の例では、idとhogeというキーを持つtableをluact()に与えており、そのうちhogeが呼び出し可能なので、aにはhogeというメッセージを送信することができます。送られたメッセージはこのテーブルが作成されたスレッド上のVMで実際にその関数が呼び出されることによって処理されます。

luact()にはtable,cdata以外にもそれらを値としてを返す関数を指定することができ、それぞれ、tableそのもの,cdataそのもの,関数の呼び出し結果、がactorのメッセージを処理する実体としてシステムに登録されます。

またtable, cdataを返すファイルやモジュールをそれぞれluact.loadとluact.requireを使ってactorにすることもできます。
``` lua
local f = luact.load('./scripts/file.lua')
local m = luact.require 'some_module'
```


### actorへのメッセージの送信
``` lua
a:hoge() -- 100を返します。
```
これはただの関数呼び出しに見えるかもしれませんが、実際はaにhoge, {} (no argument)というメッセージを送っているのです。
aはメッセージのパラメーターとして他のスレッドや他のノードに渡すことができ、そこでもここで呼び出したのと全く同様に関数呼び出しができます。
これはこの関数呼び出しが実際にはactorに対するメッセージの送信になっているからです。


### メッセージの接頭辞(prefix)
``` lua
a:hoge() -- 100を返します。結果が帰ってくるまで待ちます。(timeout 5秒)
a:timed_hoge(1.0) -- 100を返します(timeout 1秒)
a:notify_hoge() -- 非同期にメッセージを送信し、結果を待ちません。
local ev = a:async_hoge() -- 結果を待ち受け可能な非同期呼び出しを行います
-- この間で、結果を待たない処理を自由に行うことができます。
local tp, _, ok, r = luact.event.wait(nil, ev) -- ここで結果を受け取る
```
luactではメッセージ送信として呼び出す関数名にprefixをつけることで、上記のように送受信の動作を変更することができます。timed_, notify_, async_の３つが現在用意されています。

``` lua
-- もしasyncで結果を待つ処理(luact.clock.sleepや任意のactorに対するメッセージの呼び出し)
-- も含めて行いたい場合はfutureを使ってください
local future = require 'luact.future'
local f = future(a:async_hoge())
-- ここではどのような処理も自由に行うことができます。
local ok, r = f:get() -- 確実に結果が受け取れることが保証されている

```



### actorの削除
``` lua
luact.kill(a)
a:hoge() -- exception('actor_no_body')が発生します
```




supervise
===========

actorは独立したメモリ領域を持つことができるため、他のプログラムからの干渉を受けにくく、不具合が起きにくい状態ですが、それでもactorの内部処理自体の不具合により外部からのメッセージを処理できなくなることが起こり得ます。
こういったエラーは当然予期せず発生するため、この状態が継続するとそのactorが提供していた機能の可用性がなくなってしまいます。

この問題に対する完璧ではないが、悪くない解決法は、発生頻度がある程度低いエラーの場合は、再起動することで解決することが多い、という昔ながらの知恵からもたらされます。
つまり、深刻なエラーが発生した場合にはそのactorを一旦削除し、再作成してしまうのです。

このように設定されたactorは、もしなんらかの問題が発生したとしても、再作成によって初期状態に戻ることができるので
問題の発生頻度が低ければ(そしてactorの動作が永続的なステートを必要するものではなければ）、システムに過剰な負荷を与えることなく可用性を維持することができます。

luactではこの一旦作成したactorの状態を監視し、必要があれば再作成を自動的に行う機構が存在し、superviseと呼んでいます。

### superviseされたactorの作成
``` lua
local a = luact.supervise({
	id = 100,
	hoge = function (self)
		return self.id
	end,
	explode = function (self)
		luact.exception.raise('actor_error', 'bomb!')
	end,
})
```

定義されたメッセージの２つ目、explodeに注目してください。これは内部的にエラーを起こします。
superviseされていないactorの場合、explodeを呼び出した後、他のメッセージを呼び出した場合、actor_no_bodyエラーが発生します。
これはエラーによってactorが終了して削除されてしまったからです。

一方superviseされているactorの場合、explodeを呼び出した後、すぐに他のメッセージを呼び出した場合、actor_temporary_failエラーが発生するかもしれません。
このエラーはsuperviseされているactorが再起動中であることを示します。その場合、しばらくメッセージを送り続けているとやがてメッセージ送信は成功し、actorはの可用性は回復することになります。

``` lua
a:explode()
while true do
    ok, r = pcall(a.hoge, a)
    if (not ok) then
        if r:is('actor_temporary_fail') then
            luact.clock.sleep(0.1) -- actor is under restarting. wait and retry
        else
            error(r) -- maybe permanent error. report it
        end
    end
end
```

### superviseされたactorの削除
``` lua
luact.kill(a)
```
明示的にluact.killすることで、superviseに再起動をさせずにactorを削除することができます。




仮想actor
==========

*二月中の実装完了を目指して作業中の機能です。現状では同じノード上のスレッド間でこの機能を使うことができます。つまり現バージョンでは実質的な耐障害性は提供できません。

superviseは確かに有用な機能です。ですが、我々はクラウドコンピューティングの時代に生きています。つまり、プログラムを実行するノードの数が極めて多数に及ぶ可能性があり、そういったケースではプログラムを動かすノードそのものが正常に動作しなくなるということが日常的に起こる、ということです。プログラムを動かすハードウェアが故障した場合にはスーバーバイズ自体も正常に動作しないため、上記の可用性を維持する戦略は通用しません。

この問題に実際的に取り組んだのはMicrosoftで、彼らは研究の結果、[Orleans](https://github.com/dotnet/orleans)というフレームワークを開発しました。
このフレームワークはhalo4の開発時にそのクラウドサービスの実装に使われています。

彼らのアイデアの本質的な点は、従来actorにメッセージを送る時に使われる送信先の意味づけを改良した点にあります。
従来のactorモデルのフレームワークでは、送信先はactorの物理的な位置を表すものであったのに対し、Orleansで使われる送信先はactorの論理的な役割を表す名前であり、実際の処理がどこで行われるかは内部的に管理されています。（つまり送信先から実際の処理を行うノードにメッセージはルーティングされるということです）
これにより、当初メッセージを実際に処理していたノードが故障してしまった場合でもルーティングを切り替えることで外部から見て一切何かを変更することなくメッセージの処理を続けることができます。

luactにおいても、Orleansで用いられたこの優れたアイデアを導入し、ノードの故障時にも可用性を維持することができます。luactではこの新しいメッセージの送信先のことを仮想actorと呼んでいます。


### 仮想actorの作成
``` lua
luact.listen('tcp://0.0.0.0:8080') -- 仮想actorへのメッセージのリスナーを作成する
luact.register("/system/frontend", function (id)
	return {
		id = id,
		hoge = function (self)
			return self.id
		end,
	}
end, 100)
```
仮想actorにアクセスするためのリスナーの作成はユーザーに任せられています。今回はポート8080にraw tcpでアクセスする設定です。
registerの第一引数は任意の文字列が利用できますが、/で区切ったファイルパスのような文字列を推奨しています。URLとの互換性をもたせたいからです。
この第一引数のことをvidと呼んでいます。

第二引数以下は仮想actorの実体を生成するための関数とその関数に渡される引数になります。もしくは第二引数をtableにすると、luact.registerの動作を変えるためのオプションを渡すことができます。この場合第３引数以下が生成関数及び引数となります。生成関数はcdata/tableを返す必要があります。またclosureであってはいけません（検討中）。これはこの関数が複数のノードに転送されて実行される可能性があるため、closureのような、外部変数に動作を依存した関数は問題を引き起こすからです。

そのトレードオフとして、我々は/system/frontendを定義しているクラスタであれば、そのどのマシンに接続していても常にこの仮想actorにメッセージを送ることができることが保証されます。

### 仮想actorへのアクセス
``` lua
local ref = luact.ref('tcp://127.0.0.1:8080/system/frontend')
ref:hoge() -- 100を返す
```
(actorが存在しているサーバーのアドレス)(registerで渡した文字列)
がグローバルに参照できる仮想actorのIDになります。これをgvidと呼んでいます。


### ロードバランシング

仮想actorには複数のactorを割り当てることができます。仮想actorに複数のactorが割り当てられていると、luactはメッセージをラウンドロビン方式で割り当てられているactorに分配します。actorの処理が処理ごとのコンテクストを持たない場合には、この仕組みは負荷分散のために利用することができます。

たとえば下記のようなコードを動かすluactのインスタンスを複数のノードで動かし、balance.comへのアクセスをDNSラウンドロビンなどで各ノードに分散させます。

``` lua
luact.listen('tcp://0.0.0.0:8080')
luact.register("/system/frontend", {multi_actor=true}, function (id)
	return {
		id = id,
		hoge = function (self)
			return self.id
		end,
	}
end, 100)
```

すると、下記のようなvidへのメッセージ送信は各ノードに分散されて処理されることになるでしょう。

``` lua
-- このメッセージ送信は、luact.register("/system/frontend", ...)を呼び出したすべてのノードに対して均等に分散される
luact.ref('tcp://balance.com:8080/system/frontend'):hoge()
```




並列実行と同期
==============

luactではすべてのコードはtentacleと呼ばれる極めて軽量なスレッドで動作します。rubyではfiber, jsやpythonではgeneratorとよばれている機能とほぼ同様の機能であり、協調的スレッド(cooperative thread)とよばれます。通常のOSスレッドと異なり、各スレッドが処理の実行権を奪われる（プリエンプション）タイミングが限定されています。

luactにおいては以下のようなケースでプリエンプションが発生します。
- actorにメッセージを送信して、そのリプライを待つ

``` lua
local a = luact.ref('tcp://actor.com/database')
luact.tentacle(function ()
	a:put('key', 'value') -- プリエンプションが発生し、actor aから処理が戻るまで一旦処理が停止します。
	print('put done!!')
end)
```

- eventの発火を待つ

``` lua
local a = luact.ref('tcp://actor.com/database')
luact.tentacle(function ()
	local ev = a:async_get('key') -- asyncなのでここでプリエンプションは発生しない
	local alarm = luact.clock.alarm(1.0)
	local tp, obj = event.wait(nil, ev, alarm) -- ここでプリエンプションが発生する。どちらかのイベントが発火するまで処理は中断する
	if obj == ev then
		print('put done!!')
	elseif obj == alarm then
		print('timeout!!')
	end
end)
```

- tentacle.yield(...)を呼び出す
``` lua
local co = luact.tentacle(function ()
	luact.tentacle.yield() -- だれかがcoを使ってluact.tentacle.resume(co, ...)を呼び出すまでずっと中断する
end)
```

初等的な段階では、tentacleはユーザーの目からは隠蔽されているように見えるかもしれませんが、たとえば、actorによるメッセージングで呼び出された関数はすべて独立したtentacleで実行されています。yueに与えたファイルに書かれたコードもその内容を実行する関数を実行するtentacleによって処理されています。

複雑な分散処理を記述したいケースや、オブジェクトと一緒に定期的な処理をするタスクのようなものを生成したりしたい場合にはluact.tentacleをユーザーのプログラム内部から使用することができます。その場合に複数のtentacleの実行を同期させたい場合には、actorへのメッセージのasync_***呼び出しや、luact.eventによるイベントの待ち受け機能が有用です。

luact自身の複雑な分散処理、たとえばconsensus algorithmのraftやmembership managementのgossip protocolなどはtentacleを高度に利用している例です。興味がある方はluact/cluster/raft, gossip配下のファイルを見てみてください。

TODO : tentacleとeventのもう少し詳しい使い方を書く




luact API reference
===========

luactは以下の２つのモジュールを提供します。
- luact : luactの基本機能を実現する
- logger : 流行りのログ種類で色が変わるオサレなロガーです。柔軟に出力を変えたりすることができます。

### luact
#### luact.thread_id, luact.machine_id
- luactはスレッド単位で分散します。そのスレッドを一意に識別するIDとスレッドが動作しているノードを一意に識別するIDです。
- このペアはluactが実行されるすべてのスレッドで一意になります。
- 現在のところthread_idは1 ~ luactが動いているコア数, machine_idはクラスタ内のノードに割り当てられたipv4 addressになっています。

#### luact(actor_body:cdata or table or function(args...:any):cdata or table, fargs...:any):actor
- *actor_body*および２つ目以降の引数で与えられたデータ構造を使ってactorを生成する。*actor_body*がcdataかtableの場合、*actor_body*自身がactorの実体として登録される。*actor_body*が関数の場合、actor_bodyに*fargs*を渡して呼び出して得られた返り値がactorの実体として登録される。
- 登録されたactorへのメッセージ送信に利用できるactorオブジェクトを返す。

#### luact.supervise(actor_body:cdata or table or function(args...:any):cdata or table, options:table, fargs...:any):actor
- superviseされたactorを作成する
- *actor_body*と*fargs*は実際のactorの生成に使われるパラメーターで、挙動はluact()と一緒。*options*はsuperviseの挙動を決めるためのパラメーター

#### luact.register(vid:string, fn_or_opts:function(args...:any):cdata or table, fn_or_args1:any, args...:any):actor
- 仮想actorを作成する
- *vid*というidに対応する仮想actorを作成し、実際の処理をするactorを残りの引数を使って作成する。registerで作成されたactorは自動的にsuperviseされる。
- *fn_or_opts*がテーブルの場合、registerへのオプションとして解釈される。その場合*fn_or_args1*がactorを生成するための関数となる。
- *fn_or_opts*が関数の場合はこれがactorの生成をするための関数として扱われる。
- 作成されたactorを返り値として返す

#### luact.unregister(vid:string, removed:actor or nil)
- 仮想actorの削除及び、実際の処理をするactorの削除を行う
- もし*removed*がnilの場合、*vid*に対応する仮想actor自体を削除する
- もし*removed*がactorの場合、*vid*に対応する仮想actorに登録されているactorにremovedが含まれていれば削除する。削除の結果actorの数が０以下になった場合には仮想actor自体が削除される

#### luact.ref(gvid:string):virtual_actor
- *gvid*に対応する仮想actorを作成する

#### luact.tentacle
- rubyで言う所のfiber, jsで言う所のgenerator, goで言う所のgoroutineを提供します。これにより、ユーザーは、予測可能な並行性(明示的に何かを待った場合のみyieldされる)をもったスレッドをプログラムに導入することができます。
- tentacle(proc:function):tentacle
 - 与えられた*proc*を実行する新しいtentacleを作成し返す
- tentacle.cancel(t:tentacle)
 - 与えられたtentacle *t*をキャンセルする。どのようなタイミングであっても安全に終了できるがリソースの開放はgc以外は行われないので注意すること

#### luact.event
- actor:async_*method*で返されたeventを待つための仕組み
- event.wait(filter:function or nil, events...:event):(event_type:string, event:event, event_args...:any)
 - filterで発生したイベントを取捨選択しながら、最初に発生したイベントを返す
- event.join(timeout_event:event, events...:event):(array_of(event_type:string, event:event, event_args...:any))
 - timeout_eventが発火するまでに発生したすべてのeventと、発火しなかったeventはevent_typeがtimeoutになって返される
- event.select(selector)
 - goのselect構文のようにしてeventを処理するループを作成する。selectorはeventをkey/そのeventのハンドラをvalueにするテーブル

#### luact.clock
- tentacleの一定時間実行を停止したり、時刻を得たりと、時間に関係した機能を提供する
- clock.get()
 - 現在時刻を浮動小数点で得る
- clock.sleep(sec:number)
 - *sec*秒だけ現在のtentacleの実行を止める
- clock.alarm(sec:number)
 - *sec*秒後に発火するイベントを作成する。

#### luact.memory
- memoryの手動割り当てや操作など。copyはffi.copyが圧倒的に高速なので用意しません
- memory.alloc(sz:number):void*, alloc_typed(ct:ctype, sz:number):ctype*
- memory.realloc(p:void*, sz:number):void*, realloc_typed(ct:ctype, p:void*, sz:number):ctype*
 - メモリを割り当てる。managed_がprefixにつくと、gcの対象になる._typedがpostfixにつくとctypeを指定することでその型のポインタになる
- memory.free(p:void*)
 - メモリを解放する
- memory.cmp(p:const void*,q:const void*,sz:number):boolean
 - pとqの先頭sz byteを比較する。同じならtrue
- memory.move(p:void*, q:const void*, sz:number):boolean
 - qからpにsz byteをコピーする。領域が重なっていても正しく動く

#### luact.exception
- luactのエラーはすべてexception objectとして表されるが、それを生成するモジュール
- exception.raise(category:string, args...:any)
 - *category*で与えられた種類の例外オブジェクトを作り発生させる
- exception.new(category:string, args...:any):exception_object
 - *category*で与えられた種類の例外オブジェクトを作る
- exception.define(category:string, custom_behavior:table)
 - *category*で与えられた種類の例外を登録する。*custom_behavior*で例外の挙動を変更できる。raise/newは非登録の例外に対してはエラーを返す
- exception_object:is(error_name:string):boolean
 - あるexceptionにたいして、それの種類が*error_name*であるか調べます。

#### luact.listen(addr:string, opts:table):listener
- luact自身は内部のactor通信(IIOP)のためにしかserverをセットアップしません。外部からのactor通信は通信相手のアプリケーションによって使うプロトコルが変化する可能性があるため、外部からの接続のリスナはアプリケーションの作者がセットアップすべきだと考えるからです。
 - *addr*で与えられたアドレス/プロトコル/ポートでlistenするlistenerを作成する.
 - addrは以下のように記述する
```
protocol://address:port
eg)
ssl://0.0.0.0:8080 # sslで8080でlistenする
http://0.0.0.0:1111 # httpで1111でlistenする
```
現在listenできるprotocolはssh/tcp/udpです。http2は早いうちに追加される予定です

#### luact.util
- util.sprintf 
 - Cのsprintfの簡易版
- util.sleep
 - Cのnanosleepのラッパー
- util.getpid
 - Cのgetpidのラッパー
- などなど
 - 適当に探して使ってください。










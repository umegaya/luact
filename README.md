luact
=====

framework for writing robust, scalable network application, heavily inspired by celluloid(ruby) and Orleans(.NET).




概要
===

luactは複数のスレッドやマシン上で強調させてプログラムを動作させる、並行アプリケーションを容易に作成するためのフレームワークです。以下の特徴を備えています。

1. プログラミングの容易さ
[celluloid](https://github.com/celluloid/celluloid)と類似した、[アクターモデル](http://www.wikiwand.com/ja/%E3%82%A2%E3%82%AF%E3%82%BF%E3%83%BC%E3%83%A2%E3%83%87%E3%83%AB)の亜種を採用しており、アクター同士のメッセージングをアクターオブジェクトに対するメソッド呼び出しとして行います。これにより、通常のプログラミングに慣れ親しんだ人が、自然に複数のオブジェクトを連携させるのと同じ感覚でアクターモデルを利用した並行プログラミングを行えます。

ユーザーが記述するスクリプトのシンタクスは[lua5.1](lua.org/manual/5.1/)であり、初学者も容易に習得することができるでしょう。

luactは[celluloid](https://github.com/celluloid/celluloid)と似ていますが、より柔軟であり、多様なluaのオブジェクトをcelluloidよりも明快にactor化することができます。また、アクターに対するメソッド呼び出しにprefixという仕組みを導入しており、１つのメッセージを様々なやり方で呼び出すことができます。これにより、コードの分かりやすさと、柔軟な並行処理の記述を両立しています。


2. 耐障害性
celluloidと同様に、actorをsuperviseすることができ、シングルノードでのfault torelanceを提供しています。それだけでなく、Microsoft Researchが開発したOrleansのような、分散環境においてノードの故障が発生する場合でも可用性を可能な限り維持する仕組みも備えています(実装中）。


3. 速度
luactはluaJITに最適化されて実装されています。具体的には、通常この手のサーバーアプリケーションに見られるような、下層をC/C++で書いて、データをスクリプト言語のVMに受け渡すような構成ではなく、最も下層のnetwork IOのレイヤーから[luaJIT FFI](http://luajit.org/ext_ffi_tutorial.html)を多用して書かれています。また、luaJIT FFIでシステムで利用するデータ構造を定義することによって、可能な限り自前でメモリを管理しています。

これにより、a)gc負荷の削減、b)tracing JITによる最適化の効果を最大化、が達成されます。
TODO : 他とのbenchmark battle. Orleansには勝ちたい 




使い始める
========

luactのフロントエンドである[yue](http://github.com/umegaya/yue)を利用することで容易に使い始めることができます。
``` bash
git clone http://github.com/umegaya/yue.git
## 4スレッドでhello.luaを実行
cd yue && ./yue -c 4 sample/hello.lua
```

あとは[lua5.1](lua.org/manual/5.1/)の言語仕様について学んでおいてください。




アクター
=======

luactの基本的な実行単位であるアクターについて説明します。

### アクターの作成
``` lua
local a = luact({
	id = 100
	hoge = function (self)
		return self.id
	end
})
```

このaがアクターです。アクターになりうるluaのデータ型はtable(関数を値として持つ）, cdata(metatypeでテーブルと関連付けられたもの)です。
luactにはtable,cdata,左２つを返す関数を指定することができます。
またtable, cdataを返すファイルやモジュールをそれぞれluact.loadとluact.requireを使ってアクターにすることもできます。


### アクターへのメッセージの送信
``` lua
a:hoge() -- 100を返します。

```
これはただの関数呼び出しに見えるかもしれませんが、実際はaにhoge, {} (no argument)というメッセージを送っているのです。
aはやはりメッセージを通じて他のスレッドや他のノードに渡すことができ、そこでもここで呼び出したのと全く同様に関数呼び出しができます。
これはこの関数呼び出しが実際にはアクターに対するメッセージの送信になっているからです。


### アクターの削除
``` lua
luact.kill(a)
a:hoge() -- exception('actor_no_body')が発生します
```




スーパーバイズ
===========

アクターは独立したメモリ領域を持つことができるため、他のプログラムからの干渉を受けにくく、不具合が起きにくい状態ですが、
それでもアクターの内部処理自体の不具合により外部からのメッセージを処理できなくなることが起こり得ます。
こういったエラーは当然予期せず発生するため、このままではそのアクターが提供していた機能の可用性がなくなってしまいます。

この問題に対する完璧ではないが、悪くない解決法は、発生頻度がある程度低いエラーの場合は、再起動することで解決することが多い、という昔ながらの知恵からもたらされます。
つまり、深刻なエラーが発生した場合にはそのアクターを一旦削除し、再作成してしまうのです。

このように設定されたアクターは、もしなんらかの問題が発生したとしても、再作成によって初期状態に戻ることができるので
問題の発生頻度が低ければ(そしてアクターの動作が永続的なステートを必要するものではなければ）、システムに過剰な負荷を与えることなく可用性を維持することができます。

luactではこの一旦作成したアクターの状態を監視し、必要があれば再作成を行う機構をsuperviseと呼んでいます。

### superviseされたアクターの作成
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
superviseされていないアクターの場合、explodeを呼び出した後、他のメッセージを呼び出した場合、actor_no_bodyエラーが発生します。
これはエラーによってアクターが終了して削除されてしまったからです。

一方superviseされているアクターの場合、explodeを呼び出した後、すぐに他のメッセージを呼び出した場合、actor_temporary_failエラーが発生するかもしれません。
このエラーはsuperviseされているアクターが再起動中であることを示します。その場合、しばらくメッセージを送り続けているとやがてメッセージ送信は成功し、アクターはの可用性は回復することになります。

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

### superviseされたアクターの削除
``` lua
luact.kill(a)
```
明示的にluact.killすることで、superviseに再起動をさせずにアクターを削除することができます。




仮想アクター
==========

*二月中の実装完了を目指して作業中の機能です。現状では同じノード上のスレッド間でこの機能を使うことができます。

スーパーバイズは確かに有用な機能です。ですが、我々はクラウドコンピューティングの時代に生きています。つまり、プログラムを実行するノードの数が極めて多数に及ぶ可能性があり、そういったケースではプログラムを動かすノードそのものが正常に動作しなくなるということが日常的に起こる、ということです。プログラムを動かすハードウェアが故障した場合にはスーバーバイズ自体も正常に動作しないため、上記の可用性を維持する戦略は通用しません。

この問題に実際的に取り組んだのはMicrosoftで、彼らは研究の結果、[Orleans](https://github.com/dotnet/orleans)というフレームワークを開発しました。
このフレームワークはhalo4の開発時にそのクラウドサービスの実装に使われています。

彼らのアイデアの本質的な点は、従来アクターにメッセージを送る時に使われる送信先の意味づけを改良した点にあります。
従来のアクターモデルのフレームワークでは、送信先はアクターの物理的な位置を表すものであったのに対し、Orleansで使われる送信先はアクターの論理的な役割を表す名前であり、実際の処理がどこで行われるかは内部的に管理されています。（つまり送信先から実際の処理を行うノードにメッセージはルーティングされるということです）
これにより、当初メッセージを実際に処理していたノードが故障してしまった場合でもルーティングを切り替えることで外部から見て一切何かを変更することなくメッセージの処理を続けることができます。

luactにおいても、Orleansで用いられた優れたアイデアを導入し、ノードの故障時にも可用性を維持することができます。luactではこの新しいメッセージの送信先のことを仮想アクターと呼んでいます。


### 仮想アクターの作成
``` lua
luact.listen('tcp://0.0.0.0:8080')
luact.register("/system/frontend", function (id)
	return {
		id = id,
		hoge = function (self)
			return self.id
		end,
	}
end, 100)
```
仮想アクターにアクセスするためのリスナーの作成はユーザーに任せられています。今回はポート8080にraw tcpでアクセスする設定です。
registerの第一引数は任意の文字列が利用できますが、/で区切ったファイルパスのような文字列を推奨しています。ブラウザアクセスとの互換性をもたせたいからです
この第一引数のことをvidと呼んでいます。


### 仮想アクターへのアクセス
``` lua
local ref = luact.ref('tcp://127.0.0.1:8080/system/frontend')
ref:hoge() -- 100を返す
```
(アクターが存在しているサーバーのアドレス)(registerで渡した文字列)
がグローバルに参照できる仮想アクターのIDになります。これをgvidと呼んでいます。


### ロードバランシング

仮想アクターには複数のアクターを割り当てることができます。仮想アクターに複数のアクターが割り当てられていると、luactはメッセージをラウンドロビン方式で割り当てられているアクターに分配します。アクターの処理が処理ごとのコンテクストを持たない場合には、この仕組みは負荷分散のために利用することができます。

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



モジュール関数
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
- *actor_body*および２つ目以降の引数で与えられたデータ構造を使ってアクターを生成する。*actor_body*がcdataかtableの場合、*actor_body*自身がアクターの実体として登録される。*actor_body*が関数の場合、actor_bodyに*fargs*を渡して呼び出して得られた返り値がアクターの実体として登録される。
- 登録されたアクターへのメッセージ送信に利用できるアクターオブジェクトを返す。

#### luact.supervise(actor_body:cdata or table or function(args...:any):cdata or table, options:table, fargs...:any):actor
- スーパーバイズされたアクターを作成する
- *actor_body*と*fargs*は実際のactorの生成に使われるパラメーターで、挙動はluact()と一緒。*options*はsuperviseの挙動を決めるためのパラメーター

#### luact.register(vid:string, fn_or_opts:function(args...:any):cdata or table, fn_or_args1:any, args...:any):actor
- 仮想アクターを作成する
- *vid*というidに対応する仮想アクターを作成し、実際の処理をするアクターを残りの引数を使って作成する。registerで作成されたアクターは自動的にsuperviseされる。
- *fn_or_opts*がテーブルの場合、registerへのオプションとして解釈される。その場合*fn_or_args1*がアクターを生成するための関数となる。
- *fn_or_opts*が関数の場合はこれがアクターの生成をするための関数として扱われる。
- 作成されたアクターを返り値として返す

#### luact.unregister(vid:string, removed:actor or nil)
- 仮想アクターの削除及び、実際の処理をするアクターの削除を行う
- もし*removed*がnilの場合、*vid*に対応する仮想アクター自体を削除する
- もし*removed*がactorの場合、*vid*に対応する仮想アクターに登録されているアクターにremovedが含まれていれば削除する。削除の結果actorの数が０以下になった場合には仮想アクター自体が削除される

#### luact.ref(gvid:string):virtual_actor
- *gvid*に対応する仮想アクターを作成する

#### luact.tentacle
- rubyで言う所のfiber, jsで言う所のgenerator, goで言う所のgoroutineを提供します。これにより、ユーザーは、予測可能な並行性(明示的に何かを待った場合のみyieldされる)をプログラムに導入することができます。
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
- exception.new(category:string, args...:any)
 - *category*で与えられた種類の例外オブジェクトを作る
- exception.define(category:string, custom_behavior:table)
 - *category*で与えられた種類の例外を登録する。*custom_behavior*で例外の挙動を変更できる。raise/newは非登録の例外に対してはエラーを返す

#### luact.listen(addr:string, opts:table):listener
- luact自身は内部のactor通信(IIOP)のためにしかserverをセットアップしません。外部からのactor通信は通信相手のアプリケーションによって使うプロトコルが変化する可能性があるため、外部からの接続のリスナはアプリケーションの作者がセットアップすべきだと考えるからです。
 - *addr*で与えられたアドレス/プロトコル/ポートでlistenするlistenerを作成する.addrは以下のように記述する
```
protocol://address:port
eg)
ssl://0.0.0.0:8080 # sslで8080でlistenする
http://0.0.0.0:1111 # httpで1111でlistenする
```
現在listenできるprotocolはssh/tcp/udpです。http2は早いうちに追加される予定です

#### luact.util
- ゴミ箱
 - util.sprintf
 - util.sleep
 - util.getpid
 ...
 - 適当に探して使ってください。










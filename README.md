# The world beyond batch: Streaming 102
 >原文作者：Tyler Akidau  
 >译者：    董捷

## 简介
欢迎回来！如果你错过了前一篇博文，The world beyond batch: Streaming 101，我强烈建议先花一点时间阅读第一篇博文。第一篇博文介绍了我接下来讲述的这篇博文的一些必要的基础，我假设本篇博文的读者已经熟悉了前一篇博文内介绍的名词与概念。

同时，请注意本篇博文里包含了许多动画，所以如果读者直接打印的话将丢失一些精华内容。

免责申明说完了，让Party开始吧！首先简单回顾一下，上次我专注在三个主要的领域：**名词**，精确地定义了我们常说的比如“流处理”的概念；**批处理与流处理对比**，比较这两种处理模式理论上的能力，并且提出了流处理若想超越批处理，仅需考虑两件事：1.正确性 2.推理时间的工具；和**数据处理模式**，探讨了批处理与流处理在处理有界与无界数据时的基本方法。

在这篇博文中，我希望在数据处理模式这个问题上更加深入，并且利用具体个案带场景更加更具体地深入挖掘。本篇博文的结构主要包括两部分：

* **温顾Streaming 101**: 简要回顾Streaming 101博文中阐述的概念，并通过具体例子阐述之前的观点。
* **Streaming 102**：Streaming 101的伴生篇，通过一系列具体案例，使之前介绍的概念更加具体易于理解。

当介绍完这篇博文时，我将覆盖我认为打造一个健壮的乱序数据处理系统的核心原则与理念；这就是推理时间的工具，这些工具使得流处理在真正意义上超越传统批处理。

为了更好地理解现实中这些工具的应用，我将使用Dataflow SDK（也就是Google Could Dataflow的API）的片段代码帮助阐述，辅以部分展示概念的动画。我使用Dataflow SDK而不是其他一些大家可能更熟悉的SDK，比如Spark Streaming或者Storm的理由是在今天没有其他系统的API在语义上能够表达我今天所要讲述的所有案例。好消息是其他系统正在往这个方向努力。一个更好的消息是我们（Google）如今给Apache Software Foundation提交了一个提案：创建一个Apache Dataflow孵化项目（联合包括data Artisans，Cloudera，Talend与一些其他公司），希望以Dataflow提供的健壮的无序处理语义为中心，创立一个开放的社区与生态。这将使得2016年成为一个非常有趣的年份，不好意思跑题了- -

本篇博文将不会探讨上次我承诺的流处理与批处理的对比；不好意思，我错误地低估了我想要在这篇博文中探讨的内容量与整理这些内容所需花费的时间。当下，我根本无法预估加入这个章节可能造成的延期。可能带来些许安慰的是，我最终在Strata+Hadoop 2015世界大会上分享了“海量数据处理的演化”这个议题（并可能在2016伦敦Strata+Hadoop世界大会上分享更新版），其中包含了许多我希望在对比环节中涵盖的内容。分享所使用的制作精美的幻灯片可以从[这里](http://goo.gl/5k0xaL)获得，请自行下载。可以肯定的是，幻灯片内容与原先承诺的对比章节会有所不同，但好歹凑合-  -

## 回顾与路线图
在Streaming 101中，我首先澄清了一些名词。首先区分了有界与无界数据。有界数据源拥有有限的大小，也就是通常我们所谓的“批”数据。无界数据源可能在大小上是无限的，也就是通常我们所谓的“流”数据。我尝试避免使用批数据与流数据，因为他们常常伴随特定的误导且受限的含义。

之后我定义了批处理与流处理引擎的区别：批处理引擎是那些在设计时仅考虑有界数据的引擎，而流处理引擎是设计时考虑无界数据的引擎。我的目标是仅在描述处理引擎时使用“批”与“流”这两个名词。

在名词解释之后，我涵盖了两个处理无界数据时非常重要而基本的概念。我首先提出了事件时间（事件发生的时间）与处理时间（事件对处理引擎可见的时间）关键的区别。这奠定了Streaming 101提出的一个重要观点的基石：如果你同时关心正确性与事件真正发生时间这个上下文，必须在事件真正发生的事件时间的角度上，而非数据被处理的处理时间的角度上分析数据。

之后，我介绍了**窗口化**（也就是以临时的边界区隔数据集）这个概念，这是个处理理论上可能无穷无尽的无界数据的普遍方法。一些简单的窗口包括固定窗口与滑动窗口，但是一些更加复杂的窗口，比方会话窗口（由数据本身特性定义的窗口，比方由一段用户睡眠时间作为区隔，获得用户活跃时间作为会话的窗口）也有广泛的用途。

除了这两个概念，我们将更深刻地探讨三个更多的概念：

* **Watermarks**：一个Watermark是一个事件时间之前的输入已经全部完成的标记。一个带有x时间戳的Watermark有以下含义：“在事件时间x之前的所有事件均已可见”，这样，Watermark扮演了观察不可预见结束的无界数据源时数据进展这个指标的角色。

* **Trigger**：一个触发器是一种申明根据外界信号导致窗口的输出需发送的机制。触发器提供了决定何时发送窗口输出的灵活性。同时提供了窗口在变化时数据多次可见的可能性。反过来看，触发器使得多次修正窗口内的数据成为可能，这提供了根据上游数据源变化修正推测结果的能力，同时也提供了处理迟到数据的能力（比如手机传感器，当用户里离线时，他们记录了手机各种各样的信息，而后当用户重新连线时把这些离线时收集的数据继续传送上报）。

* **Accumulation**：一种Accumulation模式指定了同一个窗口内多个数据记录的关系。这些记录可能完全不相关，也就是他们作为时间轴上完全独立增量，抑或他们之间可能重叠。不同的Accumulation模式拥有不同的语义与不同的成本，因此需要从多种使用案例中寻找合适的模式。

最后，因为我希望使得大家更容易理解这些概念的关联，我们将会利用以下4个问题温故知新，这些问题对于处理所有无界数据的场景都非常关键：

* **What** results are calculated？ 这个问题的答案是处理流水线中的各类变形函数。其中包括计算求和，构建直方图，训练机器学习模型等。同样在经典批处理中也是如此。
* **Where** in event time are results calculated？这个问题的答案是处理流水线中事件时间窗的使用。其中包括Streaming 101中介绍的常见的窗口（固定窗口、滑动窗口与会话窗口），一些似乎没有使用任何窗口的场景（比如Streaming 101中介绍的一些时间无关的处理场景，同时传统的批处理也归为此类），和一些更复杂的窗口，比如限时拍卖。同时请注意窗口也涵盖了处理时间窗，如果你把消息进入系统的时间作为事件时间的话。
* **When** in processing time are results materialized？这个问题的答案是结合Watermarks与Triggers的使用。这个主题可以有无数种变形，但是最常见的使用模式是利用Watermarks划分特定窗口的边界，同时使用Triggers允许在窗口数据完整之前（为了实时估算，部分数据在窗口内数据完整前便发送结果）或之后（在Watermark仅仅作为窗口完整的大致参考的场景下，更多数据输入可能在Watermark之后到达）。
* **How** do refinements of results relate？这个问题的答案是选用了哪种Accumulation：discarding（多个结果都是独立且不同的），accumulating（后期结果依赖前期结果）和retracting（accumulating的值外加回撤之前触发的值一起发出）

我们将在接下来的部分更深入具体地研究以上的问题。对，我会使用一些带有颜色的标签去尝试使得这些问题相对应的概念更加明确，不用谢XD。（*注：请忽略，译者懒得搞颜色）。

## Streaming 101温习
首先，让我们回顾一下在Streaming 101中介绍的概念。不过这次，我们将使用具体的例子使得这些概念更加具体。

### What：transforms（变形函数）

在传统批处理中应用的变形函数解答了以下问题：“What results are calculated？”即便你们中的很多人可能对经典批处理已经非常熟悉，我们将会从批处理入手，毕竟它是我们如今新增的其他概念的基础。

在这部分，我们看一下这个例子：计算10个带有键值的整形数据(keyed integer)的sum值，以键值区分。如果你想要一个更具体的场景帮助理解，我们就举个例子：多组人玩一款手机游戏，我们要统计各组人获得的总分，我们需要将组内各个人的分数相加。你也可以想象类似地，计费与用量监控场景也是如此。

对于每个例子，我将使用Dataflow SDK的片段伪代码让处理流水线更加具体。所使用的都是伪代码，我会改变一些语法规则使得例子更清晰，同时我会省略一些详情（比方具体的I/O数据源），或简写一些类名（目前版本的Java SDK中Trigger的类名简直太啰嗦了，为了可读性我会简化它们）。除了这些小改动，它们基本上就是现实中的Dataflow SDK代码。同时，之后我也会提供感兴趣的用户可自行编译运行的真实代码。

如果你稍微了解Spark Streaming或者Flink，你将很容易看懂Dataflow的代码想要干什么。给你上一节快速强化课，Dataflow有两个基本元素：

* **PCollections**，它代表了数据集（有可能是超大的那种），在它之上，可以执行并发的变形函数（所以它名字前有个P）。
* **PTransforms**，它就是应用在PCollections之上从而产生新的PCollections。PTransforms可能是针对每个元素的变形，可能是多个数据的聚合，或者它可能是其他多个PTransforms的组合。

![transformers](https://d3ansictanv2wj.cloudfront.net/Figure-01---Transforms-daa8ad32f2995801b32dc2929f24d4ad.jpg)

如果你发现自己懵逼了，或者你想看看参考文献，可以参考[Dataflow Java SDK文档](https://cloud.google.com/dataflow/model/programming-model)。

回到例子的问题，我们假设我们从一个叫“input“的```PCollection<KV<String, Integer>>```开始，这里Strings就是团队名，```Integer```就是相应组内每个人的分数。现实中的流水线里，我们会从例如日志记录等原始数据中读取到一个```PCollection```，然后通过解析每条日志记录，把它转变为```PCollection<KV<String, Integer>>```。为了第一个例子的清晰性，我将会包括其他所有步骤的伪代码，但是在之后的例子中，我将会省略```I/O```读取与解析的部分。

如此，对于一个简单地读取单一```I/O```源的流水线，解析队名/分数对，然后计算各队的总分，我们的代码大致如下：

```
PCollection<String> raw = IO.read(...);
PCollection<KV<String, Integer>> input = raw.apply(ParDo.of(new ParseFn());
PCollection<KV<String, Integer>> scores = input
  .apply(Sum.integersPerKey());
```
> Listing 1. 求和流水线. Key/value 从单一```I/O```源读取, String作为队名，Integer作为比分。各队的比分分别求和统计。


对于所有之后的例子，在看到对应我们将要分析的逻辑流水线的伪代码之后，我们会看到一个展示获取到正确的输入之后，流水线如何运作的动画。更精确地说，我们将看到一个10个相同键值的数值作为输入的流水线的具体流程。在真实的流水线中，你可以想象在分布式的多个节点上会并行地发生类似的操作，但是为了我们这个例子，我们让事情尽可能保持简单。

每一个动画描绘了两个维度上的输入与输出：事件时间（X轴）和处理时间（Y轴），这样，流水线进展中观测到的真实时间的流逝就是从底部到顶部，就像上升的粗白线标注的那样。输入是一个个圈，圈中的数字代表每个输入具体的数值。它们开始是灰色的并将会在流水线观测到它们时改变颜色。

当流水线观测到数值时，它会在自己的状态中聚积这些值，并最终发出汇聚之后的结果作为输出。状态与输出表现为矩形，并在矩形顶部附近标注了在对应事件时间和处理时间内汇聚的数值。对于Listing 1的流水线，当执行经典批量处理时，动画如下所示（注意，点击图片将开始执行动画，动画会一直重复播放直到再次点击图片）：

<p><a href="https://fast.wistia.net/embed/iframe/24noytvllc?videoFoam=true&amp;wvideo=24noytvllc"><img src="https://embedwistia-a.akamaihd.net/deliveries/c9ce5cefab8d16db487342717cee477acffa7dfe.jpg?image_play_button_size=2x&amp;image_crop_resized=960x594&amp;image_play_button=1&amp;image_play_button_color=7b796ae0" width="400" height="247.5" style="width: 400px; height: 247.5px;"></a></p><p><a href="https://fast.wistia.net/embed/iframe/24noytvllc?videoFoam=true&amp;wvideo=24noytvllc">Figure 02 - classic batch</a></p>

因为这是个批流水线，它将会聚积状态直到它看到所有的输入（如顶部的绿色虚线所示），那时，它产出了唯一的输出51。在这个例子里，因为我们并没有指定窗口，我们计算了所有事件时间内数据的总和。因此，表示状态和输出的矩形覆盖了整个X轴。如果我们希望处理一个无界的数据源，经典批处理将无能为力。我们不能等待所有数据都到位因为他们永远不会终结。于是我们会希望一个概念，也就是窗口化，也就是我们在Streaming 101中介绍的。因此，在第二个问题的上下文中：“Where in event-time are results calculated？”，我们简单回顾下窗口化。

### Where：windowing（窗口化）

就像上次讨论的，窗口化就是把数据源根据临时边界切分为一个个的过程。通常切分窗口的策略包括固定窗口，滑动窗口与会话窗口：

![windows](https://d3ansictanv2wj.cloudfront.net/Figure-03---Windowing-442db0cda782c8bc0d8a2769423c6f37.jpg)

为了找一个更好的现实中窗口具体长什么样子的感觉，让我们讲我们整形累加流水线切分为固定的，2分钟滚动的窗口。用Dataflow SDK，仅仅稍微增加一个```Window.into```变形即可。

```
PCollection<KV<String, Integer>> scores = input
  .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2))))
  .apply(Sum.integersPerKey());
```

请谨记，窗口对于批处理与流处理提供了统一抽象，因为语义上讲，批处理仅仅是流处理的一个子问题。就这样，让我们首先用批处理引擎执行这个流水线；这个机制更为简单粗暴，同时，当我们切换到流引擎后，它将会让我们能够直观地对比两者。

<p><a href="https://fast.wistia.net/embed/iframe/v12dlvvgfh?videoFoam=true&amp;wvideo=v12dlvvgfh"><img src="https://embedwistia-a.akamaihd.net/deliveries/8f42fed76aca326248ee220b2d0e6daece412c20.jpg?image_play_button_size=2x&amp;image_crop_resized=960x594&amp;image_play_button=1&amp;image_play_button_color=7b796ae0" width="400" height="247.5" style="width: 400px; height: 247.5px;"></a></p><p><a href="https://fast.wistia.net/embed/iframe/v12dlvvgfh?videoFoam=true&amp;wvideo=v12dlvvgfh">Figure 04 - batch fixed</a></p>

就像之前那样，输入在状态中累积，直到他们被完全消费，之后将会产出输出结果。然而此时，对比之前1个结果，我们得到4个结果：每个2分钟的事件窗口都产出了一个输出。

此刻，我们回顾了Streaming 101中提出的两个主要的概念：事件时间与处理时间的关系，以及窗口化。如果我们想要更深入，我们需要加入本章节之前介绍的新概念：Watermarks，Triggers和Accumulation。在这里我们拉开了Streaming 102的篇章。

## Streaming 102

我们刚刚观察了批处理引擎中窗口化的处理流水线。但是理想中，我们同时希望我们的结果是低延时的，此外我们也希望我们能够处理无界的数据源。切换到流处理引擎是正确的一步，但是然而批处理引擎有一个事件窗口里所有输入都完整的时点（也就是当所有有界数据都被消费完的时候），我们目前缺少一个行之有效的方式决定无界数据的完整。于是我们有了Watermarks。

### When：watermakrs

Watermarks是以下问题的前半个解答：“When in processing time are results materialized?”Watermarks是事件时间领域中输入完整的临时标志。换一个角度想，它们是系统衡量事件流中消息被处理的进度与完整性的指标（在有界与无界数据源中均是如此，只不过显然它们在无界数据源的场景中更为有用）。

回忆下Streaming 101中的一张图，这里稍微做了一些调整，图里我这么描述事件时间与处理时间间的偏移：对于大多数现实中的数据处理系统来说，它是一个不停变化的函数。

![skew](https://d3ansictanv2wj.cloudfront.net/Figure_05_-_Event_Time_vs_Processing_Time-6484c65e43d1821c617bee747b7de020.png)

那条曲折的红线在现实中本质上就是Watermark；它体现了随处理时间增长，事件时间完整性的增长。概念上，你可以把Watermark理解为一个函数，```F(P)->E```，拥有一个处理时间的参数，返回一个事件时间的结果。（更确切地讲，这个函数的输入其实是Watermark观察到时整个流水线中所有上游的当前状态：输入源，缓冲区的数据，正在被处理的数据等；但是概念上，我们可以把它简单地理解为处理时间到事件时间的映射。）那个事件时间的时点，```E```，就是系统认为所有在事件时间之前发生的事件都已经可见的时点。换句话说，这是个假设：再也不会有在```E```之前的数据被系统观察到。基于Watermark的类型，完美或启发式，这个假设分别可能是硬性保证或是有根据的猜测：

* 完美的Watermarks：这种场景下，我们确切地认识输入源时，我们有可能构建完美的Watermark；在这种场景下，不可能有迟到的数据，所有数据都是提前到或准点到达。
* 启发式的Watermarks：对于许多分布式输入源来说，对数据源确切的全方位的了解是不现实的，在这种场景下次优解就是提供一个启发式的Watermark。启发式的Watermark利用全部可用的关于输入源的信息（partitions，partitions中信息的顺序性，文件的增长率等）去尽可能推断一个大概的进展。在很多情况下，这种Watermark可能会难以置信得准确。即使这样，启发式意味着有时会出错，此时就导致出现了迟到的数据。我们将会在接下来的Trigger章节学习多种处理迟到数据的机制。

Watermarks是一个迷人而复杂的话题，它可以讲上几天几夜，然而此处篇幅有限，所有更多关于它的话题只能等未来的博文讲解。现在，为了更好地理解Watermarks扮演的角色和它的一些短板，我们来看看两个流处理引擎仅仅使用Watermarks去判断流水线中窗口何时发出输出的例子。

<p><a href="https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102?wvideo=zbl7xyy294"><img src="https://embedwistia-a.akamaihd.net/deliveries/1ca8c3335e912cd17061ca15889d2e1c27098de2.jpg?image_play_button_size=2x&amp;image_crop_resized=960x344&amp;image_play_button=1&amp;image_play_button_color=7b796ae0" width="400" height="143.75" style="width: 400px; height: 143.75px;"></a></p><p><a href="https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102?wvideo=zbl7xyy294">The world beyond batch: Streaming 102 - O'Reilly Media</a></p>

在所有两个例子中，窗口都在Watermark传递到窗口末尾时发出输出。这两个处理流程的主要区别在于右边启发式的Watermarks算法没能统计到数值9，这个数值严重地改变了Watermark的走向。这两个例子阐述了Watermarks（和任何其他完整性的标记）的短板，准确地讲它们是：

* 太慢：当Watermark因为处理队列中其他待处理的数据（比如日志输入被网络带宽限制）被阻塞的时候，那么如果仅仅依靠Watermark去判断进度的话，输出就会滞后。

    就像上面左图中比较晚到达的数值9很明显地拖了后续所有窗口的后腿，即便后续的窗口内的输入都已完整。这个现象在窗口2 ```[12:02,12:04)``` 中非常明显，从该窗口从接受到第一个数值到最终输出结果大约花费了7分钟时间。然而启发性Watermark在这个问题上就相对来说就表现得比较好(大约花费了5分钟)，但是千万别想当然认为启发式Watermark就没有这个问题；上面例子只不过是我故意让窗口忽略数值9地结果而已。
    
    重点就是：当Watermark作为完整性的一个重要标志的时候，依赖完整的输入产生输出的做法从需要低延时的角度上并不可取。比如一个展示关键指标的大盘，时间窗为1小时或1天。你不会想要等整整1小时或1天才能看到大盘上的数据。这就是为什么这样的系统不使用传统批处理的一个蛋疼的原因。反而，不断根据时间推移修正结果直至产生最终结果的方式就好多了。
* 太快：当启发式Watermark错误地过早到达时，可能还有很多有效数据尚未到达，从而产生了迟到数据。这就是例子中右图展示的那样：Watermark在窗口1中数据全部到达前就已经到达了，导致窗口错误地输出了5而不是正确的14。这是启发式Watermark不可避免的天性导致的，启发式的本质就是有时候会出错。所以，仅仅依赖它们去决定何时输出结果在你非常关心正确性时是不够的。

在Streawming 101中，我明确表达了完整性标志对于一个输入为无界数据的健壮的乱序处理系统是不够的这个论点。Watermark的上述两个缺陷：太慢与太快，就是这个论点的论据。你不能仅仅通过完整性标志打造一个兼具正确性与低延时的系统。然而就是Triggers解决了Watermarks的这些缺陷。

### When：Triggers最棒的地方在于Triggers真特么就是太棒了！

Triggers就是“When in processing time are results materialized?”这个问题的后一半解答。Triggers申明了处理时间中何时窗口应该发送输出（其实Trigger本身也能够根据业务时间判断，比如依赖Watermarks）。任一次窗口输出被叫做一个窗格。

触发Trigger的信号包含：
* Watermark的推进（也就是业务时间的推进）：也就是我们在图6中看到的例子：当Watermark达到窗口尾部时，窗口的输出被发送。另一个例子是当时间窗口过期时，触发垃圾回收，我们将在之后重新回顾这个例子。
* 处理时间的推进：这在按一定的时间间隔更新数据时非常有用，因为处理时间不像业务时间，是稳定而无延时的。
* 元素数据：在接收到一定数量的元素就触发的场景非常有用。
* 特定符号标志：这个场景下，Trigger的触发依赖一个特定的符号数据（比如一次flush中的EOF标志）。

除了这个简单的依赖具体信号的Triggers，也存在相对复杂的触发逻辑，包括：
* Repetitions（重复）：在配合按处理时间按一定时间间隔更新时有奇效。
* Conjunctions（逻辑AND）：当所有子Triggers被触发时才触发（例如在收到Watermark并且收到一个特定的符号标志的情况下才触发）。
* Disjunctions（逻辑OR）：当任意子Triggers被触发时就触发（例如在收到Watermark或者收到一个特定的符号标志的情况下就触发）。
* Sequences（顺序）：当子Triggers按特定顺序被触发时才触发。

为了使Trigger这个概念更具体，我们把图6中默认选用的暗含的Trigger明确地写出来：

```
PCollection<KV<String, Integer>> scores = input
  .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2)))
               .triggering(AtWatermark()))
  .apply(Sum.integersPerKey());
```

了解了Trigger能提供什么能力之后，我们就可以继续探讨用Triggers解决Watermark太快和太慢的问题。在这两种场景中，我们本质上希望使得一个窗口的输出比较规律，或早或晚于Watermark经过窗口尾部的时间，所以我们需要某种重复Trigger。那么问题来了：什么是重复的？

在太慢的场景中（也就是获得一个早期推测的结果），我们或许应该假设：还有很多该窗口的数据尚未到达（因为如我所说是早期）。这样，随着处理时间的推移重复地触发窗口可能是明智的，因为触发条件并不依赖于窗口内的数据量；最坏的情况就是得到一个周期性触发的稳定的流。

在太快的场景中（也就是依据迟到数据修正因为启发式Watermark早到而产生的错误结果），我们假设启发式Watermark是相对准确的（一个合理的安全假设）。在这个场景中，不会有过多的迟到数据，但是一旦我们收到一条迟到数据，我们期望尽快修正之前的结果。收到1条迟到数据后就触发能够使得结果尽快得到修正（也就是在收到迟到数据时即刻触发）。但即使这样，因为迟到数据不会太频繁出现，也不会对系统造成过多的负担。

请注意我说的只不过是例子而已：我们能够根据不同的场景选择（或根本不选择）合适顺手的Trigger。

最后，我们需要谱写不同Triggers的时序合奏曲同时处理早到，准时与晚点的情况。我们能够通过一个顺序触发器（```Sequence Trigger```）和一个特殊的```OrFinally Trigger```做到，它设置了一个子Trigger，当子Trigger触发时中断父Trigger。

```
PCollection<KV<String, Integer>> scores = input
  .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2)))
               .triggering(Sequence(
                 Repeat(AtPeriod(Duration.standardMinutes(1)))
                   .OrFinally(AtWatermark()),
                 Repeat(AtCount(1))))
  .apply(Sum.integersPerKey());
```

然而这么写太啰嗦了。同时因为现实中不断早到|准点|不断晚点的情形太常见了，我们定制（但是语义上相同的）一个语法糖使得申明这种Trigger更简单清晰。

```
PCollection<KV<String, Integer>> scores = input
  .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2)))
               .triggering(
                 AtWatermark()
                   .withEarlyFirings(AtPeriod(Duration.standardMinutes(1)))
                   .withLateFirings(AtCount(1))))
  .apply(Sum.integersPerKey());
```

执行上述的任意的代码（不管使用完美Watermark或是启发式Watermark）得到的结果将大致如下：

<p><a href="https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102?wvideo=li3chq4k3t"><img src="https://embedwistia-a.akamaihd.net/deliveries/a12e5efa572e0fb6d0c5ae9d5db1094676f6ef53.jpg?image_play_button_size=2x&amp;image_crop_resized=960x344&amp;image_play_button=1&amp;image_play_button_color=7b796ae0" width="400" height="143.75" style="width: 400px; height: 143.75px;"></a></p><p><a href="https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102?wvideo=li3chq4k3t">The world beyond batch: Streaming 102 - O'Reilly Media</a></p>

这个版本的代码相对之前的有以下两个明显的优势：
* 对于之前窗口2```[12:02,12:04)```的“Watermark太慢”这种场景，我们现在每分钟发出一次早到数据的更新。这在使用完美Watermark的场景下特别有效，首次输出延时从原先的7分钟降低到大约3分半钟。然后这在使用启发式Watermark的场景下也有明显的作用。在所有两种场景下，现在均由周期性地修正结果值（窗格的数值从7，14最后变为22），并且基本上获得了从输入完整到窗格最终输出结果的最低延时。
* 对于之前窗口1```[12:00,12:02)```的“启发式Watermark太快”这种场景，当数值9最终到达时，我们立刻用它修正了窗格的输出并得到正确的结果14。

这些Triggers的一个有趣的副作用是它们使得完美Watermark与启发式Watermark的输出模式趋同。虽然之前它们的输出模式完全不同，但现在却看起来大同小异。

现在它们最大的不同就是窗口的生命周期。在完美Watermark的场景中，我们知道一旦Watermark到达，不会再有迟到的数据，所以我们可以直接清空窗口的所有状态。但是在启发式Watermark的场景中，当Watermark到达后窗口还会等待一段时间内到达的迟到数据。但是说实话，其实我们系统并不知道迟到的数据会迟到多久，也就不知道窗口得等待多久。所以我们提出了“容忍延迟(Allowed Lateness)”这个概念。

### When: 容忍延迟(垃圾回收)

在回答一下个问题（“How do refinements of results relate？”）之前，我希望探讨下不断运行的乱序无界数据处理系统所面临的一个非常现实的问题：垃圾回收。如图7的例子所示，所有窗口持久化的状态的生命周期等同于整个例子的生命周期；这是为了让我们正确地处理迟到的数据。虽然如果我们有足够的存储的话这么做非常棒，但是现实中在处理无穷无尽的无界数据源的时候，保留所有窗口的全部状态显然是不现实的；我们最终会耗尽硬盘空间。

所以，任何现实中的乱序处理系统需要以某种方式限制窗口的生命周期。一种简单有效的做法就是定义一个容忍延迟时间，也就是在事件时间域定义一个系统是否处理迟到数据的时点边界。任意在这个时点之后才到的数据均会被直接丢弃。一旦你定义了最晚处理数据的时点，那么我们就能大致估算窗口的状态需要保存多久：当Watermark大于边界时点的时候。同时你也授予系统即刻丢弃一切更迟数据的自由，意味着系统不会为没人关心的太迟的数据浪费任何存储。

因为容忍延迟与Watermark之间有一些微妙的纠葛，我们不妨用一个例子说明下。我们给使用启发性Watermark的处理流水线代码5/图7加一个容忍延迟-1分钟（这个1分钟是为迎合这个例子，现实中的情况一般是一个更大的容忍延迟）：

```
PCollection<KV<String, Integer>> scores = input
  .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2)))
               .triggering(
                 AtWatermark()
                   .withEarlyFirings(AtPeriod(Duration.standardMinutes(1)))
                   .withLateFirings(AtCount(1)))
               .withAllowedLateness(Duration.standardMinutes(1)))
  .apply(Sum.integersPerKey());
```

这时，这个流水线的执行就会类似下图8所示，为了凸显容忍延迟我特别添加以下内容：
* 本来表示处理时间的粗白线现在为所有活跃的窗口标注了延迟参考线。
* 一旦Watermark超过了延迟参考线，也就意味着窗口中的所有状态均为丢弃。在窗口关闭后，窗口曾经的生命周期以小白点围成的立方体表示，同时有一段表示延迟参考线的延伸。
* 为了这个例子，我特别为窗口1新增了数值6。数值6虽然迟到了，但是延迟没有大于延时参考线，因此数值6被用以修正窗口的输出。然后数值9的延迟过大，超过了参考线，因此被直接丢弃了。

<p><a href="https://fast.wistia.net/embed/iframe/muwcqnrmxf?videoFoam=true&amp;wvideo=muwcqnrmxf"><img src="https://embedwistia-a.akamaihd.net/deliveries/4bc76118539bfe60869ec06e6b6919b6e48d0ff2.jpg?image_play_button_size=2x&amp;image_crop_resized=960x643&amp;image_play_button=1&amp;image_play_button_color=7b796ae0" width="400" height="267.5" style="width: 400px; height: 267.5px;"></a></p><p><a href="https://fast.wistia.net/embed/iframe/muwcqnrmxf?videoFoam=true&amp;wvideo=muwcqnrmxf">Figure 08 - streaming speculative late allowed lateness 1min</a></p>

关于延迟参考线最后两个需要关注的点：
* 如果使用了完美Watermark，完全没有使用延迟参考线的必要，当然也可以把它设为可选值0s。这就是我们在图7中看到的那样。
* 需要注意的是，即使我们选择了启发式Watermark，也并非一定需要使用延迟参考线。比方说按一定的键值统计全局的聚合结果，同时键值的可能性是有限的。这时，全局的窗口数量是有限的，同时聚合结果并不会占用过多的空间，因此也不需要为限制窗口的生命周期烦恼。

可行性分析完毕，让我们进入第四个也就是最后一个问题。

### How: Accumulation（聚积）

当Trigger能够使得一个窗口产生多次窗格数据，我们就面临了最后一个问题：“How do refinements of results relate？”在之前我们看到的例子中，所有连续的窗格的数据都依赖于上一个窗格，但是其实Accumelation有多种模式。
* Discarding（丢弃）：当一个窗格发出后，丢弃所有的状态。这意味着所有相邻的窗格都是独立的。丢弃模式在下游自行聚积的场景下十分有用，比如下游系统希望每次接收增量改变，而非全量结果。
* Accumulating（聚积）：如图7所示，窗格每次发出结果，状态均被保留，稍晚的输入将被聚积到之前的状态中。这意味着任意相邻的窗格都依赖于前一个窗格。聚积模式在下游直接覆盖之前结果的场景下非常有用，就如使用BigTable或HBase直接存储键值对那样。
* Accumulating & retracting（聚积&撤销）：类似聚积模式，但是当产生一个新窗格的同时，产生对于之前窗格的一些回撤。回撤（结合新产生的聚积值）本质上是在明确地表达：“我之前跟你说结果是X，但特么我错了。别管上次的X了，把X用Y替换掉。”在两种场景下回撤相当有用：
* 当消费下游会根据不同的维度重组数据，有可能新数据与旧数据维度不同，应该被归为另一组。这样，新数据就不能直接替换老数据；相反，你需要从老数据那组回撤老数据，并在新数据那组增加新数据。
* 当使用动态窗口（也就是会话窗口）的时候，因为窗口合并，新数据可能不仅仅是替换老数据。这种场景下，新窗口自身很难决定哪些老窗口会被合并，相反的撤销所有的老窗口相对简单暴力。

把不同模式的语义放在一起比较可能对大家理解有所帮助。想想图7中窗口2```[12:02,12:04)```中的三个窗格。下表展示了使用三种不同的聚积模式时每次窗格发出的数据。

 -|丢弃|聚积|聚积&回撤
 -|-|-|- 
 窗格1 | 7 | 7 | 7
 窗格2 | 7 | 14 | 14，-7
 窗格3 | 8 | 22 | 22, -14
 最后观测的值 | 8 | 22 | 22
 总和 | 22 | 51 | 22
 
 * **丢弃模式**：所有窗格仅处理在该窗格内到达的数据。这样，最后观测的值并不是总和的值。但是，如果你把每个独立窗格的值相加，不就会得到正确的总和22。这就是为什么当下游自行处理聚积时该模式十分有用的原因。
 * **聚积模式**：如图7所示，所有窗格处理在该窗格内所有的数据，外加之前所有窗格的数值。这样，最后观测到的值就是总和22。但是如果你把所有窗口的数据相加，窗格2的结果将会被统计2遍，窗格1的结果将会被统计3遍，从而得到不正确的51。这就是为什么当消费下游直接覆盖之前数据的场景下该模式十分有用的原因。
 * **聚积&回撤模式**：所有窗格包含聚积的值与需要回撤的值，这样，不论是最后观测到的值，还是总和的值，都是正确的总和22，这就是为什么回撤这么强大的原因。
 
为了看看现实中丢弃模式的案例，我们把代码稍作修改：
```
PCollection<KV<String, Integer>> scores = input
  .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2)))
               .triggering(
                 AtWatermark()
                   .withEarlyFirings(AtPeriod(Duration.standardMinutes(1)))
                   .withLateFirings(AtCount(1)))
               .discardingFiredPanes())
  .apply(Sum.integersPerKey());
```

这段代码还是用启发式Watermark的流处理引擎执行将会产生如下输出：

<p><a href="https://fast.wistia.net/embed/iframe/64r8oawoc2?videoFoam=true&amp;wvideo=64r8oawoc2"><img src="https://embedwistia-a.akamaihd.net/deliveries/39c7bdd39bb0cff10c8650807b255f3f34fad0a8.jpg?image_play_button_size=2x&amp;image_crop_resized=960x674&amp;image_play_button=1&amp;image_play_button_color=7b796ae0" width="400" height="281.25" style="width: 400px; height: 281.25px;"></a></p><p><a href="https://fast.wistia.net/embed/iframe/64r8oawoc2?videoFoam=true&amp;wvideo=64r8oawoc2">Figure 09 - streaming speculative late discarding</a></p>

虽然大致上，输出的形状跟图7中聚积模式的形状相近，但是注意所有的窗格在丢弃模式下都不重叠。结果每个窗格都是独立输出。

如果我们希望看看用回撤的效果，我们同样可以对代码稍作修改（注意，Google Cloud Dataflow当下还在开发回撤功能，所以例子中API的名称可能与最终发布版有较大出入）：

```
PCollection<KV<String, Integer>> scores = input
  .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2)))
               .triggering(
                 AtWatermark()
                   .withEarlyFirings(AtPeriod(Duration.standardMinutes(1)))
                   .withLateFirings(AtCount(1)))
               .accumulatingAndRetractingFiredPanes())
  .apply(Sum.integersPerKey());
```

如果执行以上代码，将得到以下结果：

<p><a href="https://fast.wistia.net/embed/iframe/ksse5yiq3u?videoFoam=true&amp;wvideo=ksse5yiq3u"><img src="https://embedwistia-a.akamaihd.net/deliveries/3fef7a569ee1cf9e44c4a4859ef059c9c815fde5.jpg?image_play_button_size=2x&amp;image_crop_resized=960x674&amp;image_play_button=1&amp;image_play_button_color=7b796ae0" width="400" height="281.25" style="width: 400px; height: 281.25px;"></a></p><p><a href="https://fast.wistia.net/embed/iframe/ksse5yiq3u?videoFoam=true&amp;wvideo=ksse5yiq3u">Figure 10 - streaming speculative late retracting</a></p>

因为所有的窗格都重叠，我们比较难清晰地观察回撤。我们用红色标记回撤的值，结合一个蓝色重叠窗格产生的偏紫色的新数值。同时我稍微调整了下两个数值的位置并把它们以逗号分隔，便于大家更好地观察。

把图7、图9和图10放在一起比较能够较好地比较三者的区别：
![accumulations](https://d3ansictanv2wj.cloudfront.net/Figure11-V2-0dfcc51fc13c713a3903d1d10681955a.jpg)

你可以想象，这三种模式从左到右（丢弃模式、聚积模式、聚积&回撤模式），所消耗的计算与存储资源成本是递增的。从这个角度上，选择Accumulation模式从另一个角度上看就是权衡正确性、延时与成本。



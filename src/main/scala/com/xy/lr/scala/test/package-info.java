/**
 * Created by xylr on 16-4-12.
 * com.xy.lr.scala.test
 *
 * RDD基本转换操作：
 *  1, map[U : ClassTag] (f : T => U) : RDD[U]
 *    map 函数将 RDD 中类型为T的元素, 一对一地映射为类型为U的元素
 *
 *  2, distinct () : RDD[T]
 *    distinct 函数返回 RDD 中所有不一样的元素
 *
 *  3, flatMap [U : ClassTag] (f : T => TraversableOnce[U]) : RDD[U]
 *    flatMap 函数则是将 RDD 中的每一个元素进行一对多的转换
 *
 *  4, repartition (numPartitions : Int) : RDD[T]
 *    repartition 和 coalesce 是对 RDD 的分区进行重新划分,
 *    repartition 只是 coalesce 接口中 shuffle 为 true 的简易实现
 *
 *  5, coalesce (numPartitions : Int, shuffle : Boolean = false) : RDD[T]
 *    如上
 *
 *  6, randomSplit (weight : Array[Double],
 *        seed : Long = System.nanoTime) : Array[RDD[T]]
 *    randomSplit 函数是根据 weight 权重将一个 RDD 切分成多个 RDD
 *
 *  7, glom () : RDD[Array[T]]
 *    glom 函数是将 RDD 中每一个分区中类型为 T 的元素转换成数组 Array[T]
 *
 *  8, union (other : RDD[T]) : RDD[T]
 *    并集
 *
 *  9, intersection (other : RDD[T], partitioner : Partitioner) : RDD[T]
 *    交集
 *
 *  10, subtract (other : RDD[T], partitioner : Partitioner) : RDD[T]
 *    差集
 *
 *  11, mapPartitions [U : ClassTag] (f : Iterator[T] => Iterator[U],
 *        preservesPartitioning : Boolean = false) : RDD[U]
 *    mapPartitions 与 map 转换类似,
 *    只不过映射函数的输入参数由 RDD 中的每一个元素变成了 RDD 中每一个分区迭代器
 *
 *  12, mapPartitionsWithIndex [U : ClassTag] (f : (Int, Iterator[T]) => Iterator[U],
 *        preservesPartitioning : Boolean = false) : RDD[U]
 *    mapPartitionsWithIndex 和 mapPartitions 功能类似
 *
 *  13, zip [U : ClassTag] (other : RDD[U]) : RDD[(T, U)]
 *    zip 函数的功能是将两个 RDD 组合成为 Key/Value 形式的 RDD, 这里默认两个 RDD 的 partition
 *    数量以及元素数量都相同，否则不相同系统将会抛出异常
 *
 *  14, zipPartitions [B : ClassTag, V : ClassTag] (rdd2 : RDD[B],preservesPartitioning : Boolean)
 *        (f : (Iterator[T], Iterator[B]) => Iterator[V]) : RDD[V]
 *    zipPartitions 是将多个 RDD 按照 partition 组合成为新的 RDD, zipPartitions 需要相互组合的 RDD
 *    具有相同的分区数, 但是对于每个分区中的元素数量是没有要求的
 *
 *  15, zipWithIndex() : RDD[(T, Long)]
 *    zipWithIndex 是将 RDD 中的元素和这个元素的 ID 组合成为键值对
 *
 *  16, zipWithUniqueId() : RDD[(T, Long)]
 *    基本同上
 *
 * 键值 RDD 转换操作：
 *  1，partitionBy (partitioner : Partitioner) : RDD[(K, V)]
 *    对应repartition
 *
 *  2, mapValues[U] (f : V => U) : RDD[(K, U)]
 *    对 K, V 中的 V 值进行 map 操作
 *
 *  3, flatMapValues[U] (f : V => TraversableOnce[U]) : RDD[(K, U)]
 *    对 K, V 中的 V 值进行 flatMap 操作
 *
 *  4, combineByKey[C] (createCombiner : V => C, mergeValue : (C, V) => C,
 *        mergeCombiners : (C, C) => C, partitioner : Partitioner, mapSideCombine : Boolean = true,
 *        serializer : Serializer = null) : RDD[(K, C)]
 *    createCombiner : 创建组合器函数，将 V 类型值转换成 C 类型值
 *    mergeValue : 合并值函数，将一个 V 类型值和一个 C 类型值合并成一个 C 类型值
 *    mergeCombiner : 合并组合器函数，将两个 C 类型值合并成一个 C 类型值
 *    partitioner : 指定分区函数
 *    mapSideCombine : 布尔类型值，指定是否需要在 Map 端进行 combine 操作，类似于 MapReduce 中进行的 combine 操作
 *
 *  5, foldByKey (zeroValue : V, partitioner : Partitioner)(func : (V, V) => V) : RDD[(K, V)]
 *
 *  6, reduceByKey (func : (V, V) => V) : RDD[(K, V)]
 *
 *  7, groupByKey () : RDD[(K, Iterable[V])]
 *
 *  8, cogroup[W] (other : RDD[(K, W)]) : RDD[(K, (Iterable[V], Iterable[W]))]
 *    如下
 *
 *  9, join[W] (other : RDD[(K, W)]) : RDD[(K, (V, W))]
 *    join, leftOuterJoin, rightOuterJoin 都是针对 RDD[K, V] 中 K 值相等的连接操作，分别对应于内连接，
 *    左外连接，右外连接，最终都会调用 cogroup 类实现
 *
 *  10, leftOuterJoin[W] (other : RDD[(K, W)]) : RDD[(K, (V, Option[W]))]
 *    如上
 *
 *  11, rightOuterJoin[W] (other : RDD[(K, W)]) : RDD[(K, (Option[W], V))]
 *    如上
 *
 *  12, subtractByKey[W : ClassTag] (other : RDD[(K, W)]) : RDD[(K, V)]
 *    subtractByKey 和基本转换操作 subtract 类似，只是针对 RDD[K, V] 中的 K 值来进行操作
 */
package com.xy.lr.scala.test;
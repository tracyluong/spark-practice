import org.apache.spark.sql._

case class Video(video_id: String,
                 trending_date: String,
                 title: String,
                 channel_title: String,
                 views: BigInt = 0,
                 likes: BigInt = 0,
                 dislikes: BigInt = 0,
                 comment_count: BigInt = 0)


object Youtube {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    val videoDF1 = spark.read
      .format("csv")
      .option("header", true)
      .load("CAvideos1000.csv")
      .na
      .drop
      .withColumn("views", 'views.cast("BigInt").as("views"))
      .withColumn("likes", 'likes.cast("BigInt").as("likes"))
      .withColumn("dislikes", 'dislikes.cast("BigInt").as("dislikes"))
      .withColumn("comment_count", 'comment_count.cast("BigInt").as("comment_count"))
      .select('video_id, 'trending_date, 'title, 'channel_title, 'views, 'likes, 'dislikes, 'comment_count)

    val videoRdd1 = videoDF1
      .as[Video]
      .rdd

    val viewByChannel1 = videoRdd1
      .keyBy(v => (v.video_id, v.channel_title))
      .reduceByKey((v1, v2) =>  {
        if (v1.views > v2.views)
          v1
        else
          v2 })
      .map(x => (x._1._2, x._2.views))
      .reduceByKey(_ + _)

    println("Views by channel (another version - sample): ")
    viewByChannel1.take(5).foreach(println)

    //percentage of likes, dislikes, comment_count for each channel
    val perChannelAgg = videoRdd1
      .keyBy(v => (v.video_id, v.channel_title))
      .reduceByKey((v1, v2) =>  {
        if (v1.views > v2.views)
          v1
        else
          v2 })
      .map(x => (x._1._2, x._2))
      .reduceByKey((v1, v2) =>
        v1.copy(
          views = v1.views + v2.views,
          likes = v1.likes + v2.likes,
          dislikes = v1.dislikes + v2.dislikes,
          comment_count = v1.comment_count + v2.comment_count
        ))

    // Percentage of likes, dislikes, comments per view
    val percentage = perChannelAgg.map({
      case (channel, info) =>
        (channel, info.likes.toDouble/info.views.toDouble,
          info.dislikes.toDouble/ info.views.toDouble,
          info.comment_count.toDouble/ info.views.toDouble)
    })

    println("10 channel with highest ratio of likes")
    // Select 10 channel with most percentage of likes
    percentage
      .sortBy(_._2)
      .map(x => (x._1, x._2))
      .top(10)
      .foreach(println)

    println("10 channel with highest ratio of dislikes")
    // Select 10 channel with most percentage of dislikes
    percentage
      .sortBy(_._3)
      .map(x => (x._1, x._3))
      .top(10)
      .foreach(println)

    println("10 channel with highest ratio of comments")
    // Select 10 channel with most percentage of comm counts
    percentage
      .sortBy(_._4)
      .map(x => (x._1, x._4))
      .top(10)
      .foreach(println)
}
}
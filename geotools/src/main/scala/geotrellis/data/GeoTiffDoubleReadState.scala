package geotrellis.data

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader

import geotrellis._
import geotrellis.raster._

import org.geotools.gce


case class GtfCacheEntry(path:String, noData:Double, geoRaster:java.awt.image.Raster)

object GeoTiffCache {


  private val cacheLoader = new CacheLoader[String,GtfCacheEntry]() {

    override def load(path: String): GtfCacheEntry = {

      println(s"GeoTiffCache: Load of $path into cache")

      val reader = GeoTiff.getReader(path)

      GtfCacheEntry(
        path = path,
        noData = reader.getMetadata.getNoData.toDouble,
        geoRaster = reader.read(null).getRenderedImage.getData
      )
    }
  }

  private val cache =
    CacheBuilder.newBuilder()
      .maximumSize(100)                               // maximum 100 records can be cached
      // .expireAfterAccess(30, TimeUnit.MINUTES)     // cache will expire after 30 minutes of access
      .build(cacheLoader)


  def get(path:String) = cache.get(path)

}

case class GtfDataEntry(path:String, x:Int, y:Int, w:Int, h:Int, nodata:Double, data:Array[Double])

object GtfDataCache {

  private val cacheLoader = new CacheLoader[(String,Int,Int,Int,Int), GtfDataEntry]() {

    override def load(id:(String,Int,Int,Int,Int)): GtfDataEntry = {

      val path = id._1
      val x = id._2
      val y = id._3
      val w = id._4
      val h = id._5

      println(s"GtfDataCache: Load of $id into cache")

      val gtfDataEntry = GeoTiffCache.get(path)
      val noData = gtfDataEntry.noData
      val geoRaster = gtfDataEntry.geoRaster

      val data = Array.fill(w * h)(noData)
      geoRaster.getPixels(x, y, w, h, data)

      GtfDataEntry(
        path = path,
        x = x,
        y = y,
        w = w,
        h = h,
        nodata = noData,
        data = data
      )
    }
  }

  private val cache =
    CacheBuilder.newBuilder()
      .maximumSize(100)                               // maximum 100 records can be cached
      // .expireAfterAccess(30, TimeUnit.MINUTES)     // cache will expire after 30 minutes of access
      .build(cacheLoader)


  def get(path:String, x:Int, y:Int, w:Int, h:Int) = cache.get((path,x,y,w,h))

}


class GeoTiffDoubleReadState(path:String,
                          val rasterExtent:RasterExtent,
                          val target:RasterExtent,
                          val typ:RasterType,
                          val reader:gce.geotiff.GeoTiffReader) extends ReadState {
  def getType = typ

  private var noData:Double = 0.0
  private var data:Array[Double] = null
  
  // private def initializeNoData(reader:gce.geotiff.GeoTiffReader) = {
  //   noData = reader.getMetadata.getNoData.toDouble
  // }

  def getNoDataValue = noData

  def initSource(pos:Int, size:Int) {
    val x = 0
    val y = pos / rasterExtent.cols
    val w = rasterExtent.cols
    val h = size / rasterExtent.cols

    println("GeoTiffDoubleReadState: initSource() begin "+Thread.currentThread().getId+" "+System.currentTimeMillis())
    println(s"GeoTiffDoubleReadState: initSource() x: $x y: $y w: $w h: $h path: $path rasterExtent: $rasterExtent target: $target")

    val startReadingTime = System.currentTimeMillis()
    println("GeoTiffDoubleReadState: initSource() start reading "+Thread.currentThread().getId+" "+startReadingTime )

    val entry = GtfDataCache.get(path, x, y, w, h)
    noData = entry.nodata
    data = entry.data

    val endReadingTime = System.currentTimeMillis()
    println("GeoTiffDoubleReadState: initSource() end reading "+Thread.currentThread().getId+" duration: "+((endReadingTime-startReadingTime)/1000)+" seconds")

    /*
    val gtfDataEntry = GeoTiffCache.get(path)
    noData = gtfDataEntry.noData
    val geoRaster = gtfDataEntry.geoRaster

    // initializeNoData(reader)

    data = Array.fill(w * h)(noData)
    //val geoRaster = reader.read(null).getRenderedImage.getData

    // TODO: Cache noData and georatser in cache !!

    val endReadingTime = System.currentTimeMillis()
    println("GeoTiffDoubleReadState: initSource() end reading "+Thread.currentThread().getId+" duration: "+((endReadingTime-startReadingTime)/1000)+" seconds")

    // TODO: Uncomment this after resolving of Guava issue !!!!
    geoRaster.getPixels(x, y, w, h, data)
    */

    println("GeoTiffDoubleReadState: initSource() end "+Thread.currentThread().getId+" "+System.currentTimeMillis())
  }

  @inline
  def assignFromSource(sourceIndex:Int, dest:MutableRasterData, destIndex:Int) {
    dest.updateDouble(destIndex, data(sourceIndex))
  }

  protected[this] override def translate(rData:MutableRasterData) {
    if(noData != Double.NaN) {
      println(s"NoData value is $noData, converting to NaN")
      var i = 0
      val len = rData.length
      var conflicts = 0
      while (i < len) {
        if(rData(i) == Double.NaN) conflicts += 1
        if (rData(i) == noData) rData.updateDouble(i, Double.NaN)
        i += 1
      }
      if(conflicts > 0) {
        println(s"[WARNING]  GeoTiff contained values of ${Double.NaN}, which are considered to be NO DATA values in ARG format. There are $conflicts raster cells that are now considered NO DATA values in the converted format.")
      }
    }
  }
}

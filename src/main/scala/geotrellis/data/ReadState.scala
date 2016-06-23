package geotrellis.data

import scala.math.{Numeric, min, max, abs, round, floor, ceil}
import java.io.{File, FileInputStream, FileOutputStream}

import geotrellis._
import geotrellis.raster._
import geotrellis.process._
import geotrellis.raster.IntConstant

import spire.syntax.cfor._

trait ReadState {
  val rasterExtent:RasterExtent
  val target:RasterExtent

  /**
   * Defines the type of Raster this ReadState will read.
   */
  def getType: RasterType

  /**
   * Creates the RasterData of the resampled Raster.
   * By default creates an ArrayRasterData of the 
   * type defined by getType.
   */
  def createRasterData(cols:Int, rows:Int):MutableRasterData = RasterData.emptyByType(getType, cols, rows)

  /**
   * This function is called to initialize the source
   * data in preperation for resampling and assigning into
   * the destination RasterData.
   */
  protected[this] def initSource(position:Int, size:Int):Unit

  /**
   * Assign an indexed source value to the
   * destination RasterData at the specified index.
   */
  protected[this] def assignFromSource(sourceIndex:Int, dest:MutableRasterData, destIndex:Int):Unit

  /**
   * Creates a Raster based on the destination RasterData
   * after it has been read in.
   */
  protected[this] def createRaster(data:MutableRasterData) = Raster(data, target)

  /**
   * Called for cleanup after the ReadState is no longer used.
   */
  def destroy() {}

  /**
   * Overwrite this to translate data from source to destination,
   * for example to tranlsate NoData values.
   */
  protected[this] def translate(data:MutableRasterData): Unit = ()

  def loadRaster(): Raster = {
    val re = rasterExtent

    println("ReadState: loadRaster() begin "+Thread.currentThread().getId+" "+System.currentTimeMillis())
    println("ReadState: rasterExtent "+re+" target: "+target)

    // keep track of cell size in our source raster
    val src_cellwidth =  re.cellwidth
    val src_cellheight = re.cellheight
    val src_cols = re.cols
    val src_rows = re.rows
    val src_xmin = re.extent.xmin
    val src_ymin = re.extent.ymin
    val src_xmax = re.extent.xmax
    val src_ymax = re.extent.ymax

    // the dimensions to resample to
    val dst_cols = target.cols
    val dst_rows = target.rows

    // calculate the dst cell size
    val dst_cellwidth  = (target.extent.xmax - target.extent.xmin) / dst_cols
    val dst_cellheight = (target.extent.ymax - target.extent.ymin) / dst_rows

    // save "normalized map coordinates" for destination cell (0, 0)
    val xbase = target.extent.xmin - src_xmin + (dst_cellwidth / 2)
    val ybase = target.extent.ymax - src_ymin - (dst_cellheight / 2)

    // track height/width in map units
    val src_map_width  = src_xmax - src_xmin
    val src_map_height = src_ymax - src_ymin

    // initialize the whole raster
    // TODO: only initialize the part we will read from
    val src_size = src_rows * src_cols
    initSource(0, src_size)
    
    // this is the resampled destination array
    val dst_size = dst_cols * dst_rows
    val resampled = createRasterData(dst_cols, dst_rows)

    // these are the min and max columns we will access on this row
    val min_col = (xbase / src_cellwidth).asInstanceOf[Int]
    val max_col = ((xbase + dst_cols * dst_cellwidth) / src_cellwidth).asInstanceOf[Int]

    // start at the Y-center of the first dst grid cell
    var y = ybase

    // loop over rows
    cfor(0)(_ < dst_rows, _ + 1) { dst_row =>
      // calculate the Y grid coordinate to read from
      val src_row = (src_rows - (y / src_cellheight).asInstanceOf[Int] - 1)

      // pre-calculate some spans we'll use a bunch
      val src_span = src_row * src_cols
      val dst_span = dst_row * dst_cols

      // xyz
      if (src_span + min_col < src_size && src_span + max_col >= 0) {

        // start at the X-center of the first dst grid cell
        var x = xbase
  
        // loop over cols
        cfor(0)(_ < dst_cols, _ + 1) { dst_col =>  
          // calculate the X grid coordinate to read from
          val src_col = (x / src_cellwidth).asInstanceOf[Int]
  
          // compute src and dst indices and ASSIGN!
          val src_i = src_span + src_col

          if (src_col >= 0 && 
              src_col < src_cols && 
              src_i < src_size && 
              src_i >= 0) {
            val dst_i = dst_span + dst_col
            assignFromSource(src_i, resampled, dst_i)
          }
  
          // increase our X map coordinate
          x += dst_cellwidth
        }
      }

      // decrease our Y map coordinate
      y -= dst_cellheight
    }

    // build a raster object from our array and return
    translate(resampled)
    val res = createRaster(resampled)

    println("ReadState: loadRaster() end "+Thread.currentThread().getId+" "+System.currentTimeMillis())
    res
  }
}

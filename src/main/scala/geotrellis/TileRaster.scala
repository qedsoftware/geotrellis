package geotrellis

import geotrellis.raster._
import spire.syntax.cfor._
import scala.collection.mutable

object TileRaster {
  def apply(r:Raster,tileLayout:TileLayout):TileRaster =
    r match {
      case tr:TileRaster =>
        if(tileLayout != tr.tileLayout) {
          throw new GeoAttrsError("This raster is a tile raster with a different layout than" +
                                  " the argument tile layout." +  
                                 s" $tileLayout does not match ${tr.tileLayout}")
        }
        tr
      case _ =>
        wrap(r, tileLayout)
    }

  /** Converts a raster into a TileRaster with the given tileLayout.
    * 
    * @param        r              Raster to wrap.
    * @param        tileLayout     TileLayout of the resulting 
    *                              TileRaster.
    * @param        cropped        Set this flag to false if you
    *                              want the tiles to be ArrayRasters,
    *                              otherwise they will be CroppedRasters
    *                              with the raster 'r' as the backing raster.
    */ 
  def wrap(r:Raster,tileLayout:TileLayout, cropped:Boolean = true):TileRaster = {
    TileRaster(split(r,tileLayout, cropped),r.rasterExtent.adjustTo(tileLayout),tileLayout)
  }

  /** Splits a raster into a TileRaster into tiles.
    * 
    * @param        r              Raster to split.
    * @param        tileLayout     TileLayout defining the tiles to be 
    *                              generated.
    * @param        cropped        Set this flag to false if you
    *                              want the tiles to be ArrayRasters,
    *                              otherwise they will be CroppedRasters
    *                              with the raster 'r' as the backing raster.
    */ 
  def split(r:Raster,tileLayout:TileLayout, cropped:Boolean = true):Seq[Raster] = {
    val pCols = tileLayout.pixelCols
    val pRows = tileLayout.pixelRows

    val tiles = mutable.ListBuffer[Raster]()
    cfor(0)(_ < tileLayout.tileRows, _ + 1) { trow =>
      cfor(0)(_ < tileLayout.tileCols, _ + 1) { tcol =>
        val firstCol = tcol * pCols
        val lastCol = firstCol + pCols - 1
        val firstRow = trow * pRows
        val lastRow = firstRow + pRows - 1
        val gb = GridBounds(firstCol,firstRow,lastCol,lastRow)
        tiles += {
          if(cropped) CroppedRaster(r,gb)
          else CroppedRaster(r,gb).toArrayRaster
        }
      }
    }
    return tiles
  }
}

case class TileRaster(tiles:Seq[Raster],
                      rasterExtent:RasterExtent,
                      tileLayout:TileLayout) extends Raster {
  private val tileList = tiles.toList
  private val tileCols = tileLayout.tileCols
  private def getTile(tcol:Int,trow:Int) = tileList(trow*tileCols+tcol)

  val rasterType = tiles(0).rasterType

  def warp(target:RasterExtent) = toArrayRaster.warp(target)

  def toArrayRaster():ArrayRaster = {
    if (cols.toLong*rows.toLong > Int.MaxValue.toLong) {
      sys.error("This tiled raster is too big to convert into an array.") 
    } else {
      val data = RasterData.allocByType(rasterType,cols,rows)
      val len = cols*rows
      val tileCols = tileLayout.tileCols
      val tileRows = tileLayout.tileRows
      val pixelCols = tileLayout.pixelCols
      val pixelRows = tileLayout.pixelRows
      if(!isFloat) {
        cfor(0)(_ < tileCols, _ + 1) { tcol =>
          cfor(0)(_ < tileRows, _ + 1) { trow =>
            val tile = getTile(tcol,trow)
            cfor(0)(_ < pixelRows, _ + 1) { prow =>
              cfor(0)(_ < pixelCols, _ + 1) { pcol =>
                val acol = (pixelCols * tcol) + pcol
                val arow = (pixelRows * trow) + prow
                data.set(acol,arow,tile.get(pcol,prow))
              }
            }
          }
        }
      } else {
        cfor(0)(_ < tileCols, _ + 1) { tcol =>
          cfor(0)(_ < tileRows, _ + 1) { trow =>
            val tile = getTile(tcol,trow)
            cfor(0)(_ < pixelRows, _ + 1) { prow =>
              cfor(0)(_ < pixelCols, _ + 1) { pcol =>
                val acol = (pixelCols * tcol) + pcol
                val arow = (pixelRows * trow) + prow
                data.setDouble(acol,arow,tile.getDouble(pcol,prow))
              }
            }
          }
        }
      }
      ArrayRaster(data,rasterExtent)
    }
  }

  def toArray():Array[Int] = {
    if (cols.toLong*rows.toLong > Int.MaxValue.toLong) {
      sys.error("This tiled raster is too big to convert into an array.") 
    } else {
      val arr = Array.ofDim[Int](cols*rows)
      val len = cols*rows
      val tileCols = tileLayout.tileCols
      val tileRows = tileLayout.tileRows
      val pixelCols = tileLayout.pixelCols
      val pixelRows = tileLayout.pixelRows
      val totalCols = tileCols*pixelCols

      cfor(0)(_ < tileCols, _ + 1) { tcol =>
        cfor(0)(_ < tileRows, _ + 1) { trow =>
          val tile = getTile(tcol,trow)
          cfor(0)(_ < pixelRows, _ + 1) { prow =>
            cfor(0)(_ < pixelCols, _ + 1) { pcol =>
              val acol = (pixelCols * tcol) + pcol
              val arow = (pixelRows * trow) + prow
              arr(arow*totalCols + acol) = tile.get(pcol,prow)
            }
          }
        }
      }
      arr
    }
  }

  def toArrayDouble():Array[Double] = {
    if (cols.toLong*rows.toLong > Int.MaxValue.toLong) {
      sys.error("This tiled raster is too big to convert into an array.") 
    } else {
      val arr = Array.ofDim[Double](cols*rows)
      val len = cols*rows
      val tileCols = tileLayout.tileCols
      val tileRows = tileLayout.tileRows
      val pixelCols = tileLayout.pixelCols
      val pixelRows = tileLayout.pixelRows
      val totalCols = tileCols*pixelCols

      cfor(0)(_ < tileCols, _ + 1) { tcol =>
        cfor(0)(_ < tileRows, _ + 1) { trow =>
          val tile = getTile(tcol,trow)
          cfor(0)(_ < pixelRows, _ + 1) { prow =>
            cfor(0)(_ < pixelCols, _ + 1) { pcol =>
              val acol = (pixelCols * tcol) + pcol
              val arow = (pixelRows * trow) + prow
              arr(arow*totalCols + acol) = tile.getDouble(pcol,prow)
            }
          }
        }
      }
      arr
    }
  }

  def toArrayByte():Array[Byte] = toArrayRaster.toArrayByte

  def get(col:Int, row:Int):Int = {
    val tcol = col / tileLayout.pixelCols
    val trow = row / tileLayout.pixelRows
    val pcol = col % tileLayout.pixelCols
    val prow = row % tileLayout.pixelRows

    getTile(tcol, trow).get(pcol, prow)
  }

  def getDouble(col:Int, row:Int) = {
    val tcol = col / tileLayout.pixelCols
    val trow = row / tileLayout.pixelRows
    val pcol = col % tileLayout.pixelCols
    val prow = row % tileLayout.pixelRows
    getTile(tcol, trow).getDouble(pcol, prow)
  }

  def convert(rasterType:RasterType):Raster =
    TileRaster(tiles.map(_.convert(rasterType)),rasterExtent,tileLayout)

  def map(f: Int => Int): Raster = {
    val data = RasterData.allocByType(rasterType,cols,rows)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        data.set(col,row, get(col,row))
      }
    }
    ArrayRaster(data,rasterExtent)
  }

  def combine(r2:Raster)(f:(Int, Int) => Int):Raster = {
    if(this.rasterExtent != r2.rasterExtent) {
      throw new GeoAttrsError("Cannot combine rasters with different raster extents." +
                             s"$rasterExtent does not match ${r2.rasterExtent}")
    }
    val data = RasterData.allocByType(rasterType,cols,rows)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        data.set(col,row, f(get(col,row),r2.get(col,row)))
      }
    }
    Raster(data,rasterExtent)
  }

  def mapDouble(f:Double =>Double):Raster = {
    val data = RasterData.allocByType(rasterType,cols,rows)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        data.setDouble(col,row, getDouble(col,row))
      }
    }
    ArrayRaster(data,rasterExtent)
  }

  def combineDouble(r2:Raster)(f:(Double, Double) => Double):Raster = {
    if(this.rasterExtent != r2.rasterExtent) {
      throw new GeoAttrsError("Cannot combine rasters with different raster extents." +
                             s"$rasterExtent does not match ${r2.rasterExtent}")
    }
    val data = RasterData.allocByType(rasterType,cols,rows)
    cfor(0)(_ < rows, _ + 1) { row =>
      cfor(0)(_ < cols, _ + 1) { col =>
        data.setDouble(col,row, f(getDouble(col,row),r2.getDouble(col,row)))
      }
    }
    Raster(data,rasterExtent)
  }

  override
  def asciiDraw():String = {
    val sb = new StringBuilder
    for(tileRow <- 0 until tileLayout.tileRows) {
      for(row <- 0 until tileLayout.pixelRows) {
        for(tileCol <- 0 until tileLayout.tileCols) {
          val tile = getTile(tileCol,tileRow)

          for(col <- 0 until tileLayout.pixelCols) {
            val v = tile.get(col,row)
            val s = if(isNoData(v)) {
              "ND"
            } else {
              s"$v"
            }
            val pad = " " * math.max(6 - s.length,0)
            sb.append(s"$pad$s")
          }
          if(tileCol != tileLayout.tileCols - 1) {
            val pad = " " * 5
            sb.append(s"$pad| ")
          }
        }
        sb.append(s"\n")
      }
      if(tileRow != tileLayout.tileRows - 1) {
        val rowDiv = "-" * (6 * tileLayout.pixelCols * tileLayout.tileCols - 2) + 
                     "-" * (6 * tileLayout.tileCols)
        sb.append(s"  $rowDiv\n")
      }
    }
    sb.toString
  }
}

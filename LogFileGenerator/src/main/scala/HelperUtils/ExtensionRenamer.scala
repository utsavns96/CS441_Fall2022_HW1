package HelperUtils

import java.io.File
//Changes the extension of our mapreduce output file to .CSV
object ExtensionRenamer {
  def changeExt(path: String, jobname: String)=
    val file: File = new File(path + "\\part-00000")
    file.renameTo(new File(path + "\\"+jobname+".csv"))
}

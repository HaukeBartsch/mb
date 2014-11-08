// TODO:

// Could parse DICOM as well, encrypt most tag entries and send, on receive uncrypt
// anonymized processing pipeline

// Add interface to configure processing pipelines, this would include aseg or surface,
// do gradunwarp or not etc.
// Started implementing by supportign arguments to push ('arguments' string that is 
// send over to processZip.php)

// Make mb read and write json from stdin (first list, second pull on that list).

// Make current magickbox machine a list and do round-robin and distributed list/log.

package main

import (
       "os"
       "io"
       "io/ioutil"
       "fmt"
       "os/user"
       "time"
       "regexp"
       "strconv"
       "encoding/json"
       "math/rand"
       "archive/zip"
       "bytes"
       "net/http"
       "path/filepath"
       "mime/multipart"
       "log"
       "sort"
       "strings"
       "github.com/codegangsta/cli"
)

const (
        GET_VIEW int = iota  
        GET_DOWNLOAD int = iota
        GET_LOG int = iota
)

type Machine struct {
  Machine string
  Port string
  Sender string
  Status [][]string
}

// structure returned by getInstalledBuckets + machine and port
type Process struct{
  AETitle string
  Description string
  License string
  Name string
  Version string
  Machine string
  Port string
}

type Settings struct {
  Sender string
  Timeout time.Duration // in milliseconds
}

var globalSettings Settings

func printListOfMagickBoxes( f interface{} ) {

  b, err := json.MarshalIndent(f, "", "  ")
  if err != nil {
    fmt.Println("error:", err)
  }
  fmt.Printf("%v\n", string(b))

/*  m := f.([]interface{})
  fmt.Printf("[")
  for k2, v2 := range m {
        bb := v2.(map[string]interface{})
        machine := ""
        port := ""
        for k, v := range bb {
          switch vv := v.(type) {
          case int:
              //fmt.Printf(k, "is an int", vv)
          case string:
              // fmt.Printf("%d %v: %v\n", k2, k, vv)
            if k == "machine" {
              machine = vv
            }
            if k == "port" {
                port = vv
            }
          case []interface{}:
              //for i, u := range vv {
              //   fmt.Println(k2, " ", k, " ", i, "is string", u)
              //}
          default:
              //fmt.Println(k, "is of a type I don't know how to handle")
          }
        }
        fmt.Printf("{ \"id\": \"%d\", \"machine\": \"%v\", \"port\": \"%v\" }", k2, machine, port)
        if k2 < len(m)-1 {
            fmt.Printf(",")
        }
  }
  fmt.Printf("]\n") */

}

// get a list of lists back, each entry is [name of bucket, running, queued, workers]
func getStatus(machine string, port string) [][](string) {

  var f [][]string
  url := fmt.Sprintf("http://%v:%v/code/php/getStatus.php", machine, port)

  resp, err := http.Get(url)
  if err != nil {
    println("Error: could not query status")
    return f
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)

  // now parse the body
  json.Unmarshal(body, &f)

  return f  
}


// return all MagickBox machines known
// todo: check if they are visible as well 
func getMagickBoxes() (ms []Machine) {
  resp, err := http.Get("http://mmil.ucsd.edu/MagickBox/queryMachines.php")
  if err != nil {
    println("Error: could not query mmil.ucsd.edu")
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)

  // now parse the body
  var f []Machine
  json.Unmarshal(body, &f) // unnamed structure we will print everything that comes back

  return f
}

func getListOfJobs( reg string, ttype int) {

  var all_ms []Machine = getActiveMagickBoxes()
  var results []map[string]interface{}
  c := make(chan []map[string]interface{})

  for _,v := range all_ms {

    go func(machine string, port string) {

      url := fmt.Sprintf("http://%v:%v/code/php/getScratch.php", machine, port)

      resp, err := http.Get(url)
      if err != nil {
        return // log.Fatal("Error: could not get list of jobs for ", machine, " ", port)
      }
      defer resp.Body.Close()
      body, err := ioutil.ReadAll(resp.Body)

      // This is not good, we are time limited here and this would download all which can take much longer
      var res = parseGet(body, reg, ttype, machine, port)

      for _,v := range res {
        v["Machine"] = machine
        v["Port"] = port
      }
      c <- res
    }(v.Machine, v.Port)
  }
  timeout := time.After(globalSettings.Timeout)
  if ttype == GET_LOG { // wait longer
    timeout = time.After(globalSettings.Timeout*20)
  } else if ttype == GET_DOWNLOAD { // wait forever (scales with the number of studies we potentially download)
    timeout = time.After(time.Duration(5*60*1000) * time.Second)
  }

  loop:
  for i := 0; i < len(all_ms); i++ {
    select {
    case res := <-c:
      results = append(results, res...)
      //fmt.Println("got something from one go routing...")
      //break
    case <-timeout:
      ss := ""
      if len(all_ms)-i > 1 { ss = "s" }
      fmt.Fprintf(os.Stderr, "Warning: %v machine%s did not answer...\n", len(all_ms)-i, ss)
      break loop
    }
  }

  if ttype == GET_VIEW || ttype == GET_LOG {
    b, err := json.MarshalIndent(results, "", "  ")
    if err != nil {
      fmt.Println("Error: could not print result")
    }
    os.Stdout.Write(b)
    fmt.Println()
  }
}

func parseGet(buf []byte, reg string, ttype int, machine string, port string) ( []map[string]interface{} ) {
    var search = regexp.MustCompile(reg)
    var scratchDirReg = regexp.MustCompile("scratchdir")
    var pidReg = regexp.MustCompile("pid")
    var returned []map[string]interface{}

    var f interface{}
    err := json.Unmarshal(buf[:len(buf)], &f)
    if err != nil {
      fmt.Printf("%v\n%s\n\n", err, buf)
    }

    // get array of structs and test each value for the reg string
    m := f.([]interface{})
    //count := 0
    for _, v2 := range m {
        bb := v2.(map[string]interface{})
        var scratchdir = ""
        var pid = ""
        var foundOne = false
        for k, v := range bb {
          switch vv := v.(type) {
          case string:
              //fmt.Printf("%d %v: %v\n", k2, k, vv)
              //fmt.Println(search.MatchString(vv))
              if scratchDirReg.MatchString(k) {
                scratchdir = vv

                if ttype == GET_LOG {
                   //machine, port, _ := getDefaultMagickBox()
                   url := fmt.Sprintf("http://%v:%v/scratch/%v/processing.log", machine, port, scratchdir)

                   resp, err := http.Get(url)
                   if err != nil {
                      println("Error: could not get log for ", scratchdir)
                      break
                   }
                   defer resp.Body.Close()
                   body, err := ioutil.ReadAll(resp.Body)
                   bb["log"] = string(body)
                  
                   if search.MatchString(string(body)) {
                      foundOne = true
                   }
                }
              }
              if pidReg.MatchString(k) {
                pid = vv
              }

              if search.MatchString(vv) {
                foundOne = true
              }
          default:
              //fmt.Println(k, "is of a type I don't know how to handle")
          }
        }
        if foundOne {
          if ttype == GET_DOWNLOAD {
            filename := fmt.Sprintf("%s_%s.zip", pid, scratchdir)
            if _, err := os.Stat(filename); err == nil {
              fmt.Printf("file %s exists, skip download...\n", filename)
            } else {
              downloadFile(scratchdir, pid, machine, port)
            }
          } else { // GET_VIEW and GET_LOG
            returned = append(returned, bb)
          }
        }
    }
    return returned
}

func downloadFile(scratchdir string, pid string, machine string, port string) {

  fmt.Printf("request %s ", scratchdir)
  url := fmt.Sprintf("http://%v:%v/code/php/getOutputZip.php?folder=%s", machine, port, scratchdir)
  res, err := http.Get(url)
  if err != nil {
    log.Fatal(err)
  }
  defer res.Body.Close()

  filename := fmt.Sprintf("%s_%s.zip", pid, scratchdir)
  /*if _, err := os.Stat(filename); err == nil {
    fmt.Printf("file %s exists skip...", filename)
    return
  }*/
  fp, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
  // fmt.Println("\rwriting", fp.Name())
  defer fp.Close() // defer close


  buf := make([]byte, 131072)
  var all []byte
  for {
    n, _ := res.Body.Read(buf)
    if n == 0 {
      break
    }
    all = append(all, buf[:n]...)
    fmt.Printf("\033[2K[%3.2fmb] %s\r", float64(len(all))/1024.0/1024.0, fp.Name())
  }
  fmt.Printf("\nwrite to disk...")
  _, err = fp.Write(all[:len(all)])
  if err != nil {
    log.Println(err)
  }
  fmt.Printf("\033[2K\r")
}

func newfileUploadRequest(uri string, params map[string]string, paramName, path string) (*http.Request, error) {
  file, err := os.Open(path)
  if err != nil {
      return nil, err
  }
  defer file.Close()

  body := &bytes.Buffer{}
  writer := multipart.NewWriter(body)
  part, err := writer.CreateFormFile(paramName, filepath.Base(path))
  if err != nil {
      return nil, err
  }
  _, err = io.Copy(part, file)

  for key, val := range params {
      _ = writer.WriteField(key, val)
  }
  err = writer.Close()
  if err != nil {
      return nil, err
  }

  request, err := http.NewRequest("POST", uri, body)
  request.Header.Add("Content-Type", writer.FormDataContentType())
  return request, err
}

// interface to sort machines by lowerst load for a processing bucket
type MachineList struct{
  ms []Machine
  bucket string
}
func (a MachineList) Len() int { return len(a.ms) }
func (a MachineList) Swap(i, j int) { a.ms[i], a.ms[j] = a.ms[j], a.ms[i] }
func (a MachineList) Less(i, j int) bool {
  var load1 = -1
  var load2 = -1
  for _,v := range a.ms[i].Status {
    if len(v) != 4 {
      continue
    }
    val1,_ := strconv.Atoi(v[1])
    val2,_ := strconv.Atoi(v[2])
    load1 = load1 + (val2 + val1)
  }
  for _,v := range a.ms[j].Status {
    if len(v) != 4 {
      continue
    }
    val1,_ := strconv.Atoi(v[1])
    val2,_ := strconv.Atoi(v[2])
    load2 = load2 + (val2 + val1)
  }
  if load1 < load2 {
    return true
  }

  return false
}


// return the machine with the lowest load first
func lowestLoad( ms []Machine, aetitle string ) ( []Machine ) {

  var ll []Machine
  ch := make(chan Machine)
  for _,v := range ms {
    go func(m Machine) {
      s := getStatus(m.Machine, m.Port)
      m.Status = s
      ch <- m
    }(v)
  }
  timeout := time.After(globalSettings.Timeout) // time.After(500 * time.Millisecond)
  loop:
  for i := 0; i < len(ms); i++ {
    select {
    case  rem := <-ch:
      ll = append(ll, rem)
      break
    case <-timeout:
      fmt.Fprintf(os.Stderr, "Warning: we timed out looking for the status, some machines might have taken too long to answer\n")
      break loop
    }
  }
  var sml = MachineList{
    ms: ll,
    bucket: aetitle,
  }
  sort.Sort(sml)
  return sml.ms
}


func sendJob( aetitle string, dir string, arguments string) {

  var all_ms []Machine = getActiveMagickBoxes()
  // which machine should we use?
  // a machine which has the processing (AETitle) that is requested
  // from all the remaining machines pick one randomly (should check for load)

  // get all processing buckets from all machines
  var results []Process
  ch := make(chan []Process)
  for _, v := range all_ms {
    go func(machine string, port string) {
      var res = getInstalledBuckets(machine, port, nil)
      ch <- res
    }(v.Machine, v.Port)
  }
  timeout := time.After(globalSettings.Timeout) //time.After(1000 * time.Millisecond)
  loop:
  for i := 0; i < len(all_ms); i++ { // collect the results from all machines
    select {
    case  rem := <-ch:
      results = append(results, rem...)
      break
    case <-timeout:
      fmt.Fprintf(os.Stderr, "Warning: we timed out, some machines might have taken too long to answer\n")
      break loop
    }
  }

  // now look for the machines that provide our feature, count each machine only once
  var ms_valid []Machine
  for _,v := range results {
    if v.AETitle == aetitle {
      for _,vv := range all_ms {
        if (vv.Machine == v.Machine) && (vv.Port == v.Port) {
          found := false
          for _,vvv := range ms_valid {
            if (vvv.Machine == vv.Machine) && (vvv.Port == vv.Port) {
              found = true
              break
            }
          }
          if !found {
            ms_valid = append(ms_valid, vv)
            //fmt.Println("found a machine ", vv.Machine, " ", vv.Port)
          }
        }
      }
    }
  }
  if len(ms_valid) == 0 {
    fmt.Println("Error: no machine found that provides aetitle ", aetitle)
    return
  }
  
  ms_valid = lowestLoad( ms_valid, aetitle )
  var pick = 0
  if len(ms_valid) == 0 {
    fmt.Println("Error: could not get any machine that works, cannot push")
    return
  }
  // var pick = rand.Intn(len(ms_valid))
  machine := ms_valid[pick].Machine
  port    := ms_valid[pick].Port
  sender  := getSender()

  ss := ""
  sss := "s"
  if len(ms_valid) > 1 {
    ss = "s"
    sss = "" 
  }
  fmt.Printf("send directory \"%s\" as \"%v\" for \"%s\" processing to %s:%s (%v machine%s provide%s this bucket)\n", 
      dir, sender, aetitle, machine, port, len(ms_valid), ss, sss)

  // walk through all the files in the directory
  buf := new(bytes.Buffer)

  w:= zip.NewWriter(buf)

  count := 0
  err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
     // we should check if we have a DICOM file... (or not)
     if err != nil {
      fmt.Println("Error:",err)
      return err
     }
     if info.IsDir() {
      return err
     }
     count = count + 1
     fmt.Printf("\033[2Kadd file %v [%v]\r", path, count)

     // we could check if file is DICOM first... (seek 128bytes and see if we find "DICM")
     var filename = filepath.Base(path)
     var extension = filepath.Ext(path)
     var nameWithoutExtension = strings.TrimRight(filename, extension)
     var fname = ""
     if (extension != ".dcm") {
        isDCM := false
        // check if we have a DICOM file, add .dcm at the end
        f,err := os.Open(path)
        if err != nil {
          fmt.Printf("Warning: could not open file to find out if its a DICOM file")
        } else {
          _, err = f.Seek(128,0)
          if err != nil {
             isDCM = false
          } else {
            content := make([]byte, 4)
            n1, err := f.Read(content)
            if (n1 == 4) && (err == nil) {
              if string(content) == "DICM" {
                isDCM = true
              }
            }
          }
          f.Close()
        }
        if isDCM {
          fname = fmt.Sprintf("%v_file%05d.dcm", filename, count)
        } else {
          fname = fmt.Sprintf("%v_file%05d", filename, count)          
        }
     } else {
       fname = fmt.Sprintf("%v_file%05d_%v", nameWithoutExtension, count, extension)
     }
     f, err := w.Create(fname)
     if err != nil {
      fmt.Printf("Error: could not create file in zip", err)
     }
     data, readerr := ioutil.ReadFile(path)
     if readerr != nil {
      fmt.Printf("Error: could not read content of", path)
     }

     _, err = f.Write([]byte(data))
     if err != nil {
       fmt.Printf("Error: could not write file to zip")
     }

     return err
  })
  if err != nil {
    fmt.Println("Error: could not read directory")
    return
  }

  err = w.Close()
  if err != nil {
    fmt.Println("error:", err)
  }
  fmt.Println("")

  // write buffer to file
  workingdirectory := os.TempDir()

  fp, _ := ioutil.TempFile(workingdirectory, filepath.Base(dir) + "_")
  //fmt.Println("store data for send in", fp.Name())
  zipFilename := fp.Name()
  _, err = buf.WriteTo(fp)
  //fmt.Printf("wrote bytes to zip file %v\n", n)
  if err != nil {
    fmt.Printf("error: ", err)
  }
  fp.Close()
  defer os.Remove(zipFilename)

  // 
  // Now send the new zip-file to the processing machine
  //
  extraParams := map[string]string{
      "aetitle": aetitle,
      "description": "Send by MagickBox",
      "filename": filepath.Base(zipFilename),
      "sender": sender,
      "arguments": arguments,
  }
  url := fmt.Sprintf("http://%v:%v/code/php/processZip.php", machine, port)
  fileinfo, _ := os.Stat(zipFilename)
  fmt.Printf("Start sending compressed data... (%vmb)\n", fileinfo.Size()/1024/1024)
  request, err := newfileUploadRequest(url, extraParams, "theFile", zipFilename)
  if err != nil {
      log.Fatal(err)
  }
  client := &http.Client{}
  resp, err := client.Do(request)
  if err != nil {
      log.Fatal(err)
  } else {

    fmt.Println("done...")
    // this does not work
/*  buf := make([]byte, 1024)
    var all []byte
    for {
      n, err := resp.Body.Read(buf)
      if n == 0 {
        break
      }
      if err != nil {
        log.Fatal(err)
      }
      all = append(all, buf[:n]...)
      fmt.Printf("writing %3.2fmb\r", float64(len(all))/1024.0/1024.0)
    }
    fp.Write(all) */
    resp.Body.Close()

  }

}

// jobs will be removed from all active machines
func removeJobs( reg string ) {

  // all active machines
  var ms []Machine = getActiveMagickBoxes()
  // return values from each machine
  var removed []map[string]interface{}
  // collect the results
  c := make(chan []map[string]interface{})
  for _, v := range ms {

    go func(machine string, port string) {
      url := fmt.Sprintf("http://%v:%v/code/php/getScratch.php", machine, port)

      resp, err := http.Get(url)
      if err != nil {
        println("Error: could not get list of jobs")
      }
      defer resp.Body.Close()
      body, err := ioutil.ReadAll(resp.Body)

      var rem = parseRemove(body, reg, machine, port)
      for _,v := range rem {
        v["Machine"] = machine
        v["Port"] = port
      }
      c <- rem
    }(v.Machine, v.Port)
  }
  timeout := time.After(time.Second * 50000) // This can take a long long time
  loop:
  for i := 0; i < len(ms); i++ {
    select {
      case  rem := <-c:
        removed = append(removed, rem...)
        break
      case <-timeout:
        fmt.Fprintf(os.Stderr, "Warning: we timed out, some machines (%v) might have taken too long to answer\n", len(ms)-i)
        break loop
    }
  }

  b, err := json.MarshalIndent(removed, "", "  ")
  if err != nil {
    fmt.Println("error:", err)
  }
  os.Stdout.Write(b)
}

// returns the entries that have been removed
func parseRemove(buf []byte, reg string, machine string, port string) ( removed []map[string]interface{} ) {
    var search = regexp.MustCompile(reg)
    var scratchDirReg = regexp.MustCompile("scratchdir")
    var pidReg = regexp.MustCompile("pid")
    var returned []map[string]interface{}

    var f interface{}
    err := json.Unmarshal(buf[:len(buf)], &f)
    if err != nil {
      fmt.Printf("%v\n%s\n\n", err, buf)
    }

    //fmt.Printf("[")

    // get array of structs and test each value for the reg string
    m := f.([]interface{})
    //count := 0
    for _, v2 := range m {
        bb := v2.(map[string]interface{})
        var scratchdir = ""
        var pid = ""
        var foundOne = false
        for k, v := range bb {
          switch vv := v.(type) {
          case string:
              //fmt.Printf("%d %v: %v\n", k2, k, vv)
              //fmt.Println(search.MatchString(vv))
              if scratchDirReg.MatchString(k) {
                scratchdir = vv
              }
              if pidReg.MatchString(k) {
                pid = vv
              }

              if search.MatchString(vv) {
                foundOne = true
              }
          default:
              //fmt.Println(k, "is of a type I don't know how to handle")
          }
        }
        if foundOne {
            removeFile(scratchdir, pid, machine, port)
            returned = append(returned, bb)
        }
    }
    return returned
}

func removeFile(scratchdir string, pid string, machine string, port string) {

  url := fmt.Sprintf("http://%v:%v/code/php/deleteStudy.php?scratchdir=%s", machine, port, scratchdir)

  timeout := globalSettings.Timeout // time.Duration(5 * time.Second)
  client := http.Client{ Timeout: timeout }
  resp, err := client.Get(url)
  if err != nil {
    println("Error: could not reach machine ", machine, ":", port, "to delete", scratchdir)
  }

  defer resp.Body.Close()
}

func saveActiveMagickBoxes( ms []Machine ) {
  usr,_ := user.Current()
  dir := usr.HomeDir + "/.magickbox"
  b, err := json.MarshalIndent(ms, "", "  ")
  if err != nil { 
    panic(err) 
  }

  fi, err := os.Create(dir)
  if err != nil { 
    panic(err) 
  }
  defer func() {
    if err := fi.Close(); err != nil {
      panic(err)
    }
  }()
  n := len(b)
  if _, err := fi.Write(b[:n]); err != nil {
    panic(err)
  }
}

func getActiveMagickBoxes() ( []Machine ) {

  var ms = []Machine{}

  usr,_ := user.Current()
  dir := usr.HomeDir + "/.magickbox"
  fi, err := os.Open(dir)
  if err != nil {
    // could be our first time, there is no .mb file, lets create a dummy file
    //log.Println("Error: configuration incomplete, call queryMachines/selectMachine/sender first...")
    // saveDefaultMagickBox( "unknown", "unknown", "unknown" )
    var machines []Machine = []Machine{}
    saveActiveMagickBoxes( machines )

    fi, err = os.Open(dir)
    if err != nil {
      log.Fatal("Error: Could not create default mb file at", dir)
    }
    return ms
  }
  defer func() {
    if err := fi.Close(); err != nil {
      log.Fatal(err)
    }
  }()
 
  buf := make([]byte, 1024)
  n, err := fi.Read(buf)
  if err != nil {
    panic(err)
  }
  err = json.Unmarshal(buf[:n], &ms)
  if err != nil {
    fmt.Println("Error: no default machine setup ->", err)
  }
  if len(ms) == 0 {
    fmt.Println("Warning: no active machines defined, use 'mb queryMachines' followed by 'mb activeMachines add <ip> <port>' to create some")
  }

  return ms
}

func loadSettings () Settings {
  var m Settings
  usr,_ := user.Current()
  dir := usr.HomeDir + "/.mb"
  fi, err := os.Open(dir)
  if err != nil {
    // saveSetting("Sender", "unknown")
    // there is no file yet, lets create one
    var s Settings
    // use some default values if nothing has been specified
    s.Sender = "unknown"
    v, _ := time.ParseDuration("1s")
    s.Timeout = v
    //s.Timeout = time.Duration(1000) * time.Millisecond
    // fmt.Println(s.Timeout, "was the timeout that we save now")

    b, err := json.MarshalIndent(s, "", "  ")
    if err != nil { panic(err) }
    fi, err := os.Create(dir)
    if err != nil { panic(err) }
    n := len(b)
    if _, err := fi.Write(b[:n]); err != nil {
      panic(err)
    }
    fi.Close()
    // fmt.Printf(" as text this is: %s", string(b))

    fi, err = os.Open(dir)
    if err != nil {
      log.Fatal("Error: Could not create default mb file at", dir)
    }
    return m
  }
  defer func() {
    if err := fi.Close(); err != nil {
      log.Fatal(err)
    }
  }()
  buf := make([]byte, 10024)
  n, err := fi.Read(buf)
  if err != nil {
    panic(err)
  }
  err = json.Unmarshal(buf[:n], &m)
  if err != nil {
    fmt.Println("Error: no default machine setup ->", err)
  }
  return m
}

func saveSetting ( name string, value string ) {
  usr,_ := user.Current()
  dir := usr.HomeDir + "/.mb"
  // if file exists load it here
  var s Settings
  if _, err := os.Stat(dir); !os.IsNotExist(err) {
     s = loadSettings()
  }
  // now add our values
  if name == "Sender" {
    s.Sender = value
  } else if name == "Timeout" {
    v, _ := time.ParseDuration(value)
    s.Timeout = v
  }

  b, err := json.MarshalIndent(s, "", "  ")
  if err != nil { panic(err) }

  fi, err := os.Create(dir)
  if err != nil { panic(err) }
  defer func() {
    if err := fi.Close(); err != nil {
      panic(err)
    }
  }()
  n := len(b)
  if _, err := fi.Write(b[:n]); err != nil {
    panic(err)
  }

  // re-read the global settings
  globalSettings = loadSettings()
}


func saveSender( sender string ) {

  saveSetting("Sender", sender)
}

func getSender() (sender string) {
  s := loadSettings()
  return s.Sender
}

func getInstalledBuckets(machine string, port string, filter *regexp.Regexp) ([]Process) {

  var results []Process
  url := fmt.Sprintf("http://%v:%v/code/php/getInstalledBuckets.php", machine, port)

  resp, err := http.Get(url)
  if err != nil {
    println("Error: could not read installed buckets on", machine, ":", port)
    return results
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    println("Error: could not read response from machine")
  }

  var f []Process
  err = json.Unmarshal(body[:len(body)], &f)
  if err != nil {
    fmt.Printf("%v\n%s\n\n", err, body)
  }

  for _, vv := range f {
    vv.Machine = machine
    vv.Port = port

    // check if we have a filter argument
    if filter != nil {
      if filter.MatchString(vv.Machine) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Port) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.AETitle) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Description) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.License) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Name) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Version) {
        results = append(results, vv)
        break                    
      }
    } else { // by default add all
      results = append(results, vv)                
    }
  }
  return results
}


func main() {
     rand.Seed(time.Now().UTC().UnixNano())

     globalSettings = loadSettings()

     app := cli.NewApp()
     app.Name = "mb"
     app.Usage = "MagickBox command shell for query, send, retrieve, and deletion of data.\n\n" +
                 "   Setup: Start by listing known MagickBox instances (queryMachines). Identify your machines\n" +
                 "     and add them using 'activeMachines add'. They will be used for all future commands.\n\n" +
                 "     Add your own identity using the setSender command. You can also add your projects name\n" +
                 "     (see example below) to make it easier to identify your session later.\n\n" +
                 "   Most calls return textual output in JSON format that can be processed by tools\n" +
                 "   such as jq (http://stedolan.github.io/jq/).\n\n" +
                 "   Regular expressions are used to identify individual sessions. They can be supplied as\n" +
                 "   an additional argument to commands like list, log, push, pull, and remove.\n" +
                 "   Only if a session matches, the command will be applied.\n\n" +
                 "   If data is send (push) and there is more than 1 machine available that provide that type of processing\n" +
                 "   one of them will be selected based on load. Commands such as 'list' will return the machine and port\n" +
                 "   used for that session.\n\n" +
                 "   Example:\n" +
                 "     > mb setSender \"hauke:testproject\"\n" +
                 "     > mb push data_01/\n" +
                 "     > mb push data_02/\n" +
                 "     > mb list hauke:testproject\n" +
                 "     > mb pull hauke:testproject\n"
     app.Version = "0.0.4"
     app.Author = "Hauke Bartsch"
     app.Email = "HaukeBartsch@gmail.com"
     app.Flags = []cli.Flag {
      cli.StringFlag {
        Name: "config-sender",
        Value: "",
        Usage: "Identify yourself, value is used as AETitleCaller [--config-sender <string>]",
      },
     }

     // remember the default action (needs to be called if all the configurations fail to print help)
     defaultAction := app.Action
     app.Action = func(c *cli.Context) {
       if len(c.String("config-sender")) > 0  {
          if len(c.Args()) == 1 {
             saveSender( c.Args()[0] )
          } else {
             sender := getSender()
             fmt.Printf("{\"sender\": \"%s\"}\n", sender)
          }
       } else {
         defaultAction(c)
       }
     }

     app.Commands = []cli.Command{
     {
        Name:      "pull",
        ShortName: "g",
        Usage:     "Retrieve matching jobs [pull <regular expression>]",
        Description: "Download matching jobs as a zip file into the current directory.\n\n" +
                     "   If matching jobs are on more than one machine one download session per\n" +
                     "   machine will be used to retrieve the results.\n\n" +
                     "   Supply a regular expression to specify which session data to download.\n" +
                     "   Example:\n" +
                     "   > mb pull ip44\n" +
                     "   Downloads all sessions send from ip44.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 1 {
            fmt.Printf("Error: indiscriminate downloads are not supported, supply a regular expression that is matched against all fields\n")
          } else {
            getListOfJobs( c.Args().First(), GET_DOWNLOAD)
          }
        },
     },
     {
        Name:      "push",
        ShortName: "p",
        Usage:     "Send a directory for processing [push <aetitle> <dicom directory> [<arguments>]]",
        Description: "Send a directory with DICOM data to the MagickBox for processing.\n" +
                     "   The <aetitle> is used to specify what type of processing will be run.\n\n" +
                     "   Example:\n" +
                     "   > mb push ProcFS53 /space/data/DICOMS/Subj001/\n" +
                     "   Sends the specified directory for FreeSurfer processing to the currently defined default MB instance.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 2 {
            fmt.Printf("Error: don't know what to send (usage: <aetitle> <directory to send> [<arguments>])\n")
          } else {
            if len(c.Args()) == 2 {
               sendJob( c.Args()[0], c.Args()[1], "" )
            } else {
               sendJob( c.Args()[0], c.Args()[1], c.Args()[2] )              
            }
          }
        },
     },
     {
        Name:      "remove",
        ShortName: "r",
        Usage:     "Remove data [remove <regular expression>]",
        Description: "Remove session data stored in MagickBox.\n\n" +
                     "   Example:\n" +
                     "   > mb remove tmp.1234567\n" +
                     "   Removes a specific study identified by its scratchdir.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 1 {
            fmt.Printf("Error: It is not allowed to remove indiscriminately sessions from MagickBox, provide a regular expression.\n")
          } else {
            removeJobs( c.Args()[0] )
          }
        },
     },
     {
        Name:      "list",
        ShortName: "l",
        Usage:     "Show list of matching jobs [list [regular expression]]",
        Description: "Display a list of matching jobs.\n\n" +
                     "   Example:\n" +
                     "   > mb list\n" +
                     "   Returns a list of all jobs as JSON.\n\n" +
                     "   Example:\n" +
                     "   > mb list ip44\n" +
                     "   Returns a list of all jobs that have been send from ip44.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
            getListOfJobs( c.Args()[0], GET_VIEW )
          } else {
            getListOfJobs( ".*", GET_VIEW )
          }
        },
     },
     {
        Name:      "log",
        ShortName: "l",
        Usage:     "Show processing log of matching jobs [log [regular expression]]",
        Description: "Display a list of matching jobs with log entries. This is very similar to the list\n" +
                     "   command but requires a separate call to MagickBox for each job (might be slow).\n\n" +
                     "   Example:\n" +
                     "   > mb log\n" +
                     "   Returns all processing logs in json format.\n\n" +
                     "   Example:\n" +
                     "   > mb log ip44\n" +
                     "   Returns a list of all processing logs that have been send from ip44.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
            getListOfJobs( c.Args()[0], GET_LOG )
          } else {
            getListOfJobs( ".*", GET_LOG )
          }
        },
     },
     {
        Name:      "queryMachines",
        ShortName: "q",
        Usage:      "Display list of known MagickBox instances [queryMachines]",
        Description: "Display a list of all known MagickBox machines. This feature uses\n" +
                     "   a centralized service hosted at the MMIL.\n\n" +
                     "   If [with status] is supplied as an argument each machine will be queried\n" +
                     "   and all machines will contain status values for each processing bucket\n" +
                     "   [name, number of running, number of queued, number of processing slots].\n",
        Action: func(c *cli.Context) {
            ms := getMagickBoxes()
            var ms_with_status []Machine
            if (len(c.Args()) == 2) && (c.Args()[0] == "with") && (c.Args()[1] == "status") {
              ch := make(chan Machine)
              for _,v := range ms {
                go func(m Machine) {
                  s := getStatus(m.Machine, m.Port)
                  m.Status = s
                  ch <- m
                }(v)
              }
              timeout := time.After(globalSettings.Timeout) //time.After(500 * time.Millisecond)
              loop:
              for i := 0; i < len(ms); i++ {
                select {
                case  rem := <-ch:
                  ms_with_status = append(ms_with_status, rem)
                  break
                case <-timeout:
                  fmt.Fprintf(os.Stderr, "Warning: we timed out looking for the status, some machines might have taken too long to answer\n")
                  break loop
                }
              }
            }
            for _,v := range ms {
              found := false
              for _,vv := range ms_with_status {
                if (vv.Machine == v.Machine) && (vv.Port == v.Port) {
                  found = true
                  break
                }
              }
              if !found {
                ms_with_status = append(ms_with_status, v)
              }
            }
            printListOfMagickBoxes( ms_with_status )
        },
     },
/*     {
        Name:      "setMachine",
        ShortName: "s",
        Usage:      "Specify the default MagickBox [setMachine [<IP> <port>]]",
        Description: "Without any arguments this call will return the default MagickBox.\n" +
                     "   Specify the internet address (IP) and the port number to change the default\n" +
                     "   MagickBox used for subsequent calls of this tool.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 2 {
             _, _, sender := getDefaultMagickBox()
             saveDefaultMagickBox( c.Args()[0], c.Args()[1], sender )
          } else {
            machine, port, _ := getDefaultMagickBox()
            fmt.Printf("{\"machine\": \"%s\", \"port\": \"%s\"}\n", machine, port)
          }
        },
     }, */
     {
        Name:      "setSender",
        ShortName: "w",
        Usage:      "Specify a string identifying the sender [setSender [<sender>]]",
        Description: "Without any arguments this call will return the current sender.\n" +
                     "   Specify the sender string used as AETitle of the caller for subsequent calls of this tool.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
             saveSender( c.Args()[0] )
          } else {
            sender := getSender()
            fmt.Printf("{\"sender\": \"%s\"}\n", sender)
          }
        },
     },
     {
        Name:      "setSetting",
        Usage:      "Get or overwrite a program setting [setSetting [<name> | <name> <value>]]",
        Description: "Without any arguments this call will return all settings.\n" +
                     "   Specify the setting and its new value to save it.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
             s := loadSettings()
             if c.Args()[0] == "Sender" {
               fmt.Printf("{\"sender\": \"%s\"}\n", s.Sender)
             } else if c.Args()[0] == "Timeout" {
               fmt.Printf("{\"Timeout\": \"%s\"}\n", s.Timeout.String())              
             } else {
               fmt.Printf("Error: unknown setting\n")
             }
          } else if len(c.Args()) == 2 {
             if c.Args()[0] == "Sender" {
               saveSetting("Sender", c.Args()[1])
             } else if c.Args()[0] == "Timeout" {
               v, _ := time.ParseDuration(c.Args()[1])
               saveSetting("Timeout", v.String())
             } else {
               fmt.Println("Error: unknown setting")
             }
          } else {
             s := loadSettings()
             v, _ := json.MarshalIndent(s, "", "  ")
             fmt.Printf("%s\n", v)           
          }
        },
     },
     {
        Name:      "computeModules",
        ShortName: "c",
        Usage:      "Get list of buckets for the active machines",
        Action: func(c *cli.Context) {
          var ms []Machine = getActiveMagickBoxes()

          var filter *regexp.Regexp = nil
          if len(c.Args()) == 1 {
            filter = regexp.MustCompile(c.Args()[0])
          }

          var results []Process
          ch := make(chan []Process)
          for _, v := range ms {
            go func(machine string, port string) {
              var res = getInstalledBuckets(machine, port, filter)
              ch <- res
            }(v.Machine, v.Port)
          }
          timeout := time.After(globalSettings.Timeout) //time.After(1000 * time.Millisecond)
          loop:
          for i := 0; i < len(ms); i++ { // collect the results from all machines
            select {
            case  rem := <-ch:
              results = append(results, rem...)
              break
            case <-timeout:
              fmt.Fprintf(os.Stderr, "Warning: we timed out, some machines might have taken too long to answer\n")
              break loop
            }
          }

          b, err := json.MarshalIndent(results, "", "  ")
          if err != nil {
              fmt.Println("error:", err)
           }
           fmt.Printf("%v\n", string(b))
        },
     },
     {
        Name:      "activeMachines",
        ShortName: "a",
        Usage:      "Get list of active magick box machines",
        Description: "Without an argument returns list of active magick box machines.\n" +
                     "   use 'add <ip> <port>' and 'rm <ip> <port>' to add or remove machines.",
        Action: func(c *cli.Context) {

          // read from local store
          var ms []Machine = getActiveMagickBoxes()

          if len(c.Args()) > 0 {
            if len(c.Args()) == 3 {
              switch c.Args()[0] {
                case "add":
                  // find out if that entry is already part of the active MBs
                  for _, v := range ms {
                     if (v.Machine == c.Args()[1]) && (v.Port == c.Args()[2]) {
                        fmt.Println("info: machine already part of the active list, nothing will be done")
                        return
                     }
                  }

                  ms = append(ms, Machine{Machine: c.Args()[1], Port: c.Args()[2]})
                  saveActiveMagickBoxes( ms )
                case "rm":
                  //fmt.Println("rm here")
                  // find out if that entry is part of the active MBs
                  var idx = -1
                  for k, v := range ms {
                     if (v.Machine == c.Args()[1]) && (v.Port == c.Args()[2]) {
                       idx = k
                       break
                     }
                  }
                  if idx == -1 {
                     fmt.Println("Error: machine not part of activeMachines")
                     return
                  }
                  ms = append(ms[:idx], ms[idx+1:]...)
                  saveActiveMagickBoxes( ms )
                default:
                  if (c.Args()[0] != "add") && (c.Args()[0] != "rm") {
                    fmt.Println("Error: only add and rm are supported")
                    return
                  }
              }
            } else {
              fmt.Println("error: unknown arguments to activeMachines")
              return
            }
          }

          // print out the result
          b, err := json.MarshalIndent(ms, "", "  ")
          if err != nil {
            fmt.Println("error:", err)
          }
          fmt.Printf("%v\n", string(b))

        },
     },
  }

  app.Run(os.Args)
}
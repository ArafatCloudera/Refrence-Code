    

public enum Flags {
  hSync,
  hFlush,
}

main(){
  
    boolean flush = false;
    boolean sync = false;
    Flags type = Flags.valueOf(flag);
    switch (type) {
    case hSync:
      sync = true;
      break;
    case hFlush:
      flush = true;
      break;
    default:
      throw new IllegalArgumentException(
          flag + " is not a valid benchmarkType.");
    }
}

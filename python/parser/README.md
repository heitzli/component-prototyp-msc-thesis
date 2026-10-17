# README

![UnibaSQL](./UnibaSQL.png)

```bash
export ANTLR4_TOOLS_ANTLR_VERSION=4.13.2

# Generate antrl code into generated folder
antlr4 -Dlanguage=Python3 -visitor -no-listener -o generated UnibaSQL.g4
```

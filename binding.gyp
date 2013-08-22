{
  "targets": [
    {
      "target_name": "nodeora",
      "sources": [ "src/nodeora.cpp"],
      "conditions": [
        ["OS!='win'", {
          "variables": {
"oci_include_dir%": "<!(if [ -z $OCI_INCLUDE_DIR ]; then echo \"/opt/instantclient/sdk/include/\"; else echo $OCI_INCLUDE_DIR; fi)",
"oci_lib_dir%": "<!(if [ -z $OCI_LIB_DIR ]; then echo \"/opt/instantclient/\"; else echo $OCI_LIB_DIR; fi)",
"oci_version%": "<!(if [ -z $OCI_VERSION ]; then echo 11; else echo $OCI_VERSION; fi)"
          },
          "libraries": [ "-locci", "-lclntsh", "-lnnz<(oci_version)" ],
          "link_settings": {"libraries": [ '-L<(oci_lib_dir)'] }
        }],
        ["OS=='win'", {
"configurations": {
          "Release": {
            "msvs_settings": {
              "VCCLCompilerTool": {
                "RuntimeLibrary": "2"
              }
            },
}
},
          "variables": {
            "oci_include_dir%": "<!(IF DEFINED OCI_INCLUDE_DIR (echo %OCI_INCLUDE_DIR%) ELSE (echo C:\oracle\instantclient\sdk\include))",
            "oci_lib_dir%": "<!(IF DEFINED OCI_LIB_DIR (echo %OCI_LIB_DIR%) ELSE (echo C:\oracle\instantclient\sdk\lib\msvc\\vc10))",
            "oci_version%": "<!(IF DEFINED OCI_VERSION (echo %OCI_VERSION%) ELSE (echo 11))"
         },
         # "libraries": [ "-loci" ],
         "link_settings": {"libraries": [ '<(oci_lib_dir)\oraocci<(oci_version).lib'] }
        }]
      ],
      "include_dirs": [ "<(oci_include_dir)" ],
      "cflags!": [ "-fno-exceptions" ],
      "cflags_cc!": [ "-fno-exceptions" ]
    }
  ]
}


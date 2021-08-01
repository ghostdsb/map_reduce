import Config

config :map_reduce,
 :mr_name,
  String.to_atom(System.get_env("MR_NAME") || "master")
config :map_reduce,
 :mr_mode,
  String.to_atom(System.get_env("MR_MODE") || "master")

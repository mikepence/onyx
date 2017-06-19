(ns onyx.state.protocol.db)

(defprotocol State
  (put-extent! [this window-id group extent v])
  (get-extent [this window-id group extent])
  (delete-extent! [this window-id group extent])
  (put-trigger! [this window-id trigger-id group v])
  (get-trigger [this window-id trigger-id group])
  (get-all [this])
  (groups [this window-id])
  (group-extents [this window-id group])
  (drop! [this])
  (export [this window-id])
  (restore! [this window-id bs]))

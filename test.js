"use strict";

const MqemitterKafka = require("./");
const { test } = require("node:test");
const abstractTests = require("mqemitter/abstractTest.js");

abstractTests({
  builder: function(opts) {
    return MqemitterKafka(opts);
  },
  test
});
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const { merge } = require("webpack-merge");
const MiniCssExtractPlugin = require("mini-css-extract-plugin");
const TerserPlugin = require("terser-webpack-plugin");
const commonConfig = require("./webpack.config.js");

const devConfig = merge(commonConfig, {
  mode: "production",
  devtool: false, // Disable source maps to reduce memory

  module: {
    rules: [
      {
        test: /\.css$/,
        use: [MiniCssExtractPlugin.loader, "css-loader"]
      }
    ]
  },

  optimization: {
    minimize: true,
    minimizer: [
      new TerserPlugin({
        parallel: false, // Disable parallel to avoid segfault
        terserOptions: {
          compress: {
            drop_console: false,
          },
          mangle: true,
        },
      }),
    ],
    splitChunks: {
      chunks: 'all',
      maxSize: 244000, // Split large chunks
      cacheGroups: {
        vendor: {
          test: /[\\/]node_modules[\\/]/,
          name: 'vendors',
          priority: 10,
        },
      },
    },
  },

  performance: {
    hints: false, // Disable performance hints
    maxEntrypointSize: 512000,
    maxAssetSize: 512000,
  },

  plugins: [
    new MiniCssExtractPlugin({
      filename: "styles/[name].[contenthash].css",
      chunkFilename: "styles/[id].[contenthash].css"
    })
  ],

  // Limit parallelism
  parallelism: 1,
});

module.exports = devConfig;

// SPDX-License-Identifier: Apache-2.0
// Copyright 2022 Jussi Maki

package stream

type Tuple2[V1, V2 any] struct {
	V1 V1
	V2 V2
}

type Tuple3[V1, V2, V3 any] struct {
	V1 V1
	V2 V2
	V3 V3
}

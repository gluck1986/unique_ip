package ru.gluck1986.unique.ip.utils

class BitArrayLong(size: Long) {
    private var bits: IntArray? = null

    fun getBit(pos: Long): Boolean {

        return bits!![(pos / WORD_SIZE).toInt()] and (1 shl (pos % WORD_SIZE).toInt()) != 0
    }

    fun setBit(pos: Long, b: Boolean) {

        var word = bits!![(pos / WORD_SIZE).toInt()]
        val posBit = 1 shl (pos % WORD_SIZE).toInt()
        word = if (b) {
            word or posBit
        } else {
            word and ALL_ONES - posBit
        }
        bits!![(pos / WORD_SIZE).toInt()] = word


    }

    companion object {
        private const val ALL_ONES = -0x1
        private const val WORD_SIZE = 32
        private const val MAX_SIZE = 68_719_476_704 //max array size multiple 32
    }

    init {
        if (size > MAX_SIZE) {
            throw Exception("max size is $MAX_SIZE bits")
        }
        bits = IntArray((size / WORD_SIZE).toInt() + if (size % WORD_SIZE == 0L) 0 else 1)
    }
}
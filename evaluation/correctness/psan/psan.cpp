extern "C"
{
    void test_simple();
    void test_checkpoint();
    void test_cas();
    void test_queue_O0();
}

int main()
{
    // TODO: Choose test using argument

    // test_simple();
    // test_checkpoint();
    // test_cas();
    test_queue_O0();
}

from com.lingenhag.rrp.main import hello

def test_hello_default():
    assert hello() == "Hello, World!"

def test_hello_custom():
    assert hello("BFH") == "Hello, BFH!"

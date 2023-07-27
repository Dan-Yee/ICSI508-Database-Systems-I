/**
 * Driver class for estimating the size of joining two relations
 * @author Dan Yee
 */
public class JoinSize {

    public static void main(String[] args) throws Exception
    {
        if (args.length < 2)
            throw new Exception("There must be at least 2 arguments (table names) to run this program.");
        Database db = new Database(args[0], args[1]);
        db.joinSize();
    }

}
